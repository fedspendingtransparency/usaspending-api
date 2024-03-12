import itertools
import logging

import boto3
import numpy as np
import psycopg2

from argparse import ArgumentTypeError
from django import db
from django.core.management.base import BaseCommand
from math import ceil
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Optional, List
from datetime import datetime

from usaspending_api.broker.helpers.last_delta_table_load_version import (
    get_last_delta_table_load_versions,
    update_last_staging_load_version,
)
from usaspending_api.common.csv_stream_s3_to_pg import copy_csvs_from_s3_to_pg
from usaspending_api.common.etl.spark import convert_array_cols_to_string
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
    get_jdbc_connection_properties,
    get_usas_jdbc_url,
)
from usaspending_api.config import CONFIG
from usaspending_api.settings import DEFAULT_TEXT_SEARCH_CONFIG

from usaspending_api.etl.management.commands.load_query_to_delta import TABLE_SPEC

# Note: the `delta` type is not actually in Spark SQL. It's how we're temporarily storing the data before converting it
#       to the proper postgres type, since pySpark doesn't automatically support this conversion.
SPECIAL_TYPES_MAPPING = {
    db.models.UUIDField: {"postgres": "UUID USING {column_name}::UUID", "delta": "TEXT"},
    "UUID": {"postgres": "UUID USING {column_name}::UUID", "delta": "TEXT"},
    db.models.JSONField: {"postgres": "JSONB using {column_name}::JSON", "delta": "TEXT"},
    "JSONB": {"postgres": "JSONB using {column_name}::JSON", "delta": "TEXT"},
}

# 25k - 50k seems a good sweet spot from testing, but leaving it to this because expecting concurrent table writes
_SPARK_CSV_WRITE_TO_PG_MAX_RECORDS_PER_FILE = CONFIG.SPARK_PARTITION_ROWS

# Give more memory to each connection during the COPY operation of large files to avoid spillage to disk
_PG_WORK_MEM_FOR_LARGE_CSV_COPY = 256 * 1024  # MiB of work_mem * KiBs in 1 MiB


class Command(BaseCommand):

    logger: logging.Logger

    help = """
    This command reads data from a Delta table and copies it into a corresponding Postgres database table (under a
    temp name). As of now, it only supports a full reload of a table. If the table with the chosen temp name already
    exists, all existing data will be deleted before new data is written. Note this only loads in the data
    without accounting for the metadata, so make sure to run the command `copy_table_metadata` after this is complete
    if a new table has been made.
    """

    def add_arguments(self, parser):
        parser.add_argument(
            "--delta-table",
            type=str,
            required=True,
            help="The source Delta Table to read the data",
            choices=list(TABLE_SPEC),
        )
        parser.add_argument(
            "--alt-delta-db",
            type=str,
            required=False,
            help="An alternate delta database (aka schema) in which to load, overriding the TABLE_SPEC db",
        )
        parser.add_argument(
            "--alt-delta-name",
            type=str,
            required=False,
            help="An alternate delta table name to load, overriding the TABLE_SPEC destination_table" "name",
        )
        parser.add_argument(
            "--jdbc-inserts",
            action="store_true",
            help="If present, use DataFrame.write.jdbc(...) to write the Delta-table-wrapping DataFrame "
            "directly to the Postgres table using JDBC INSERT statements. Otherwise, the faster strategy using "
            "SQL bulk COPY command will be used, with the Delta table transformed to CSV files first.",
        )
        parser.add_argument(
            "--keep-csv-files",
            action="store_true",
            help="Only valid/used when --jdbc-inserts is not provided. Whether to prevent overwriting of the temporary "
            "CSV files by a subsequent write to the same output path. This will instead put them in a "
            "timestamped sub-folder of that output path (which must not already exist or an error will be "
            "thrown). Defaults to False. If setting to True, be mindful of cleaning up these preserved files on "
            "occasion.",
        )
        parser.add_argument(
            "--spark-s3-bucket",
            type=str,
            required=False,
            default=CONFIG.SPARK_S3_BUCKET,
            help="The destination bucket in S3 to write the data",
        )
        parser.add_argument(
            "--reset-sequence",
            action="store_true",
            help="In the case of a Postgres sequence for the provided 'delta-table' the sequence will be reset to 1. "
            "If the job fails for some unexpected reason then the sequence will be reset to the previous value. "
            "Should not be used with the incremental flag.",
        )
        parser.add_argument(
            "--incremental",
            action="store_true",
            help="Instead of writing the full table to temporary tables in Postgres, use Change Data Feed to "
            "determine changes, and write those to staging tables in Postgres.",
        )
        parser.add_argument(
            "--incremental-threshold",
            type=float,
            default=0.5,
            help="A percentage represented as a decimal controlling the threshold. When the ratio of changed records to "
            "total records in the `delta_table` surpasses this threshold, this command will fall back on performing a "
            "full table load, even if the `--incremental` flag is used.",
        )

    def _split_dfs(self, df, special_columns):
        """Split a DataFrame into DataFrame subsets based on presence of NULL values in certain special columns

        Unfortunately, pySpark with the JDBC doesn't handle UUIDs/JSON well.
        In addition to using "stringype": "unspecified", it can't handle null values in the UUID columns
        The only way to get around this is to split the dataframe into smaller chunks filtering out the null values.
        In said smaller chunks, where it would be null, we simply drop the column entirely from the insert,
        resorting to the default null values.
        """
        if not special_columns:
            return [df]

        # Caching for performance
        df = df.cache()

        # Figure all the possible combos of filters
        filter_batches = []
        for subset in itertools.product([True, False], repeat=len(special_columns)):
            filter_batches.append({col: subset[i] for i, col in enumerate(special_columns)})

        # Generate all the split dfs based on the filter batches
        split_dfs = []
        for filter_batch in filter_batches:
            # Apply the filters (True = null column, drop it. False = not null column, keep it)
            modified_filters = [df[col].isNull() if val else df[col].isNotNull() for col, val in filter_batch.items()]
            split_df = df.filter(np.bitwise_and.reduce(modified_filters))

            # Drop the columns where it's null **after filtering them out**
            drop_cols = [drop_col for drop_col, val in filter_batch.items() if val]
            split_df = split_df.drop(*drop_cols)

            split_dfs.append(split_df)
        return split_dfs

    def handle(self, *args, **options):
        extra_conf = {
            # Config for Delta Lake tables and SQL. Need these to keep Dela table metadata in the metastore
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # See comment below about old date and time values cannot be parsed without these
            "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
            "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
            "spark.sql.jsonGenerator.ignoreNullFields": "false",  # keep nulls in our json
        }

        spark = get_active_spark_session()
        spark_created_by_command = False
        if not spark:
            spark_created_by_command = True
            spark = configure_spark_session(**extra_conf, spark_context=spark)  # type: SparkSession

        # Setup Logger
        self.logger = logging.getLogger("script")

        # Resolve Parameters
        self.options = options
        source_delta_table = self.options["delta_table"]
        self.table_spec = TABLE_SPEC[source_delta_table]

        # Delta side
        source_delta_table_name = self.options["alt_delta_name"] or source_delta_table
        source_delta_database = self.options["alt_delta_db"] or self.table_spec["destination_database"]
        self.qualified_source_delta_table = f"{source_delta_database}.{source_delta_table_name}"
        delta_table_load_version_key = self.table_spec["delta_table_load_version_key"]

        # Postgres side
        postgres_schema = self.table_spec["swap_schema"]
        postgres_schema_def = self.table_spec["source_schema"]
        self.postgres_table_name = self.table_spec["swap_table"]
        self.qualified_postgres_table = f"{postgres_schema}.{self.postgres_table_name}"
        postgres_temp_schema = "temp"

        # Determine the latest version of the Delta Table at the time this command is running. This will later
        # be stored as the latest version written to the staging table, regardless of whether we update incrementally
        # or repopulate the entire table
        last_update_version = (
            spark.sql(f"DESCRIBE HISTORY {self.qualified_source_delta_table}").select("version").first()[0]
        )

        updated_incrementally = False

        if self.options["incremental"]:
            # Validate that necessary TABLE_SPEC fields are present for incremental loads
            if not ("incremental_delete_temp_schema" and "delta_table_load_version_key"):
                self.logger.error(
                    f"TABLE_SPEC configuration for {source_delta_table} is not sufficient for incremental loads"
                )
                self.logger.error(
                    f"Values are required for fields: `incremental_delete_temp_schema` and `delta_table_load_version_key`"
                )
                raise ArgumentTypeError(
                    f"TABLE_SPEC configuration for {source_delta_table} is not sufficient for incremental loads"
                )

            # Retrieve information about which versions of the Delta Table have been loaded where. We'll use
            # the `last_live_version` to query changes because we want all the data that has not persisted
            # to the live table, regardless of what has been previously written to the staging table because it
            # will be overwritten.
            last_staging_version, last_live_version = get_last_delta_table_load_versions(delta_table_load_version_key)

            # Create a Dataframe with a unique list of entities (based on the table's unique key) that have been
            # updated since this command was last run. If an entity has been updated multiple times, the latest
            # update will be selected. This utilizes Delta Lake's Change Data Feed feature.
            # This will be used to determine if the number of updated records exceeds the update threshold, and
            # (if not exceeding) it will serve as the basis for dataframes to be written to Postgres with incremental
            # changes.
            unique_identifiers = self.table_spec["unique_identifiers"]
            distinct_df = spark.sql(
                f"""
                SELECT * FROM (
                    SELECT * ,
                    ROW_NUMBER() OVER (PARTITION BY {', '.join(unique_identifiers)} ORDER BY _commit_version DESC) AS row_num
                    FROM table_changes('{self.qualified_source_delta_table}', {last_live_version + 1})
                    WHERE _change_type IN ('insert', 'update_postimage', 'delete')
                ) WHERE row_num = 1
            """
            )

            if not self._surpassed_update_threshold(spark, distinct_df):
                upsert_table_suffix = "temp_upserts"
                delete_table_suffix = "temp_deletes"

                # Upsert Schema (matches base table)
                qualified_upsert_temp_table = self._prepare_temp_table(postgres_temp_schema, upsert_table_suffix)

                # Delete Schema
                delete_schema_def = self.table_spec["incremental_delete_temp_schema"]
                qualified_delete_temp_table = self._prepare_temp_table(
                    postgres_temp_schema, delete_table_suffix, schema_override=delete_schema_def
                )

                # Split the Dataframe into two based on the type of update that was made
                upsert_df = distinct_df.filter("_change_type IN ('insert', 'update_postimage')")
                delete_df = distinct_df.filter("_change_type IN ('delete')")

                self._write_to_postgres(
                    spark, upsert_df, qualified_upsert_temp_table, postgres_schema_def, self.table_spec["column_names"]
                )
                self._write_to_postgres(
                    spark, delete_df, qualified_delete_temp_table, delete_schema_def, list(delete_schema_def)
                )

                updated_incrementally = True

        if not updated_incrementally:
            temp_table_suffix = "temp"

            qualified_temp_table = self._prepare_temp_table(postgres_temp_schema, temp_table_suffix)

            # Read from Delta
            df = spark.table(self.qualified_source_delta_table)

            # Reset the sequence before load for a table if it exists
            if self.options["reset_sequence"] and self.table_spec.get("postgres_seq_name"):
                postgres_seq_last_value = self._set_sequence_value(self.table_spec["postgres_seq_name"])
            else:
                postgres_seq_last_value = None

            self._write_to_postgres(
                spark,
                df,
                qualified_temp_table,
                postgres_schema_def,
                self.table_spec["column_names"],
                postgres_seq_last_value,
            )

            self.logger.info(
                f"Note: this has merely loaded the data from Delta. For various reasons, we've separated the"
                f" metadata portion of the table download to a separate script. If not already done so,"
                f" please run the following additional command to complete the process: "
                f" 'copy_table_metadata --source-table {self.qualified_postgres_table} --dest-table {qualified_temp_table}'."
            )

        if delta_table_load_version_key:
            update_last_staging_load_version(delta_table_load_version_key, last_update_version)

        # We're done with spark at this point
        if spark_created_by_command:
            spark.stop()

    def _surpassed_update_threshold(self, spark: SparkSession, df: DataFrame) -> bool:
        """
        Determines whether the number of records in the incoming DF exceeds the threshold of allowable
        changes when compared to total number records in the table being copied. The threshold percentage
        is based on the `incremental_parameter` parameter for the job.

        Args:
            spark: the active SparkSession to work within
            distinct_df: Dataframe containing changed records from table
        Returns:
            threshold_surpassed: Whether or not the number of changes has exceeded the threshold
        """

        threshold = self.options["incremental_threshold"]
        threshold_surpassed = False

        total_record_count = spark.sql(f"SELECT COUNT(*) FROM {self.qualified_source_delta_table}").first()[0]

        records_changed = df.count()

        if records_changed / total_record_count > threshold:
            self.logger.warning(f"Exceeded threshold of {threshold:.0%} for incremental updates")
            self.logger.warning(f"---- Total Records  in  {self.qualified_source_delta_table}: {total_record_count}")
            self.logger.warning(f"---- Records Changed in {self.qualified_source_delta_table}: {total_record_count}")
            threshold_surpassed = True

        return threshold_surpassed

    def _prepare_temp_table(self, temp_schema: str, temp_table_suffix: str, schema_override=None):
        """
        Creates a temporary table in the Postgres database. This table will either be based on the
        the Delta Table being copied from's corresponding Postgres table (`self.qualified_postgres_table`)
        or a provided `schema_override` schema.

        NOTES:
        - If the temporary table already exists, it will be dropped and recreated.
        - If the corresponding Postgres table is partitioned, the temporary table will also be partitioned

        Args:
            temp_schema: Destination schema in Postgres in which to create the temporary table
            temp_table_suffix: String appended to table name to differentiate between tables
            schema_override: Key value pairs with column names and column types (respectively) to define
                the schema of the temporary table. If not provided, the table will be based on the schema of the
                Delta table being copied from's corresponding Postgres table
        """

        temp_table_suffix_appendage = f"_{temp_table_suffix}"
        temp_table_name = f"{self.postgres_table_name}{temp_table_suffix_appendage}"
        qualified_temp_table = f"{temp_schema}.{temp_table_name}"

        # Checking if the temp destination table already exists
        temp_dest_table_exists_sql = f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = '{temp_schema}'
                    AND table_name = '{temp_table_name}')
        """
        with db.connection.cursor() as cursor:
            cursor.execute(temp_dest_table_exists_sql)
            temp_dest_table_exists = cursor.fetchone()[0]

        # If it does, drop it first
        if temp_dest_table_exists:
            self.logger.info(f"{qualified_temp_table} already exists. Dropping first.")
            # If the schema has changed and we need to do a complete reload, just drop the table and rebuild it
            clear_table_sql = f"DROP TABLE {qualified_temp_table}"
            with db.connection.cursor() as cursor:
                cursor.execute(clear_table_sql)
            self.logger.info(f"{qualified_temp_table} dropped.")

        is_postgres_table_partitioned = self.table_spec.get("postgres_partition_spec") is not None

        # Create the temporary table. Spark's df.write automatically does this but doesn't account for
        # the extra metadata (indexes, constraints, defaults) which CREATE TABLE X LIKE Y accounts for.
        # If there is no qualified_postgres_table to base it on, it just relies on spark to make it and work with delta table
        partition_clause = ""
        storage_parameters = "WITH (autovacuum_enabled=FALSE)"
        partitions_sql = []
        if is_postgres_table_partitioned:
            partition_clause = (
                f"PARTITION BY {self.table_spec['postgres_partition_spec']['partitioning_form']}"
                f"({', '.join(self.table_spec['postgres_partition_spec']['partition_keys'])})"
            )
            storage_parameters = ""
            partitions_sql = [
                (
                    f"CREATE TABLE "
                    # Below: e.g. my_tbl_temp -> my_tbl_part_temp
                    f"{qualified_temp_table[:-len(temp_table_suffix_appendage)]}{pt['table_suffix']}{temp_table_suffix_appendage} "
                    f"PARTITION OF {qualified_temp_table} {pt['partitioning_clause']} "
                    f"{storage_parameters}"
                )
                for pt in self.table_spec["postgres_partition_spec"]["partitions"]
            ]
        if schema_override:
            create_temp_sql = f"""
                CREATE TABLE {qualified_temp_table} (
                    {", ".join([f'{key} {val}' for key, val in schema_override.items()])}
                ) {partition_clause} {storage_parameters}
            """
        elif self.qualified_postgres_table:
            create_temp_sql = f"""
                CREATE TABLE {qualified_temp_table} (
                    LIKE {self.qualified_postgres_table} INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING IDENTITY
                ) {partition_clause} {storage_parameters}
            """
        else:
            raise RuntimeError(
                "Neither a postgres_table or postgres_schema_def is populated for the target "
                "delta table in the TABLE_SPEC and no schema override is provided"
            )
        with db.connection.cursor() as cursor:
            self.logger.info(f"Creating {qualified_temp_table}")
            cursor.execute(create_temp_sql)
            self.logger.info(f"{qualified_temp_table} created.")

            if is_postgres_table_partitioned and len(partitions_sql) > 1:
                for create_partition in partitions_sql:
                    self.logger.info(f"Creating partition of {qualified_temp_table} with SQL:\n{create_partition}")
                    cursor.execute(create_partition)
                    self.logger.info("Partition created.")

            # If there are vectors, add the triggers that will populate them based on other calls
            # NOTE: Undetermined whether tsvector triggers can be applied on partitioned tables,
            #       at the top-level virtual/partitioned table (versus having to apply on each partition)
            tsvectors = self.table_spec.get("tsvectors") or {}
            for tsvector_name, derived_from_cols in tsvectors.items():
                self.logger.info(
                    f"To prevent any confusion or duplicates, dropping the trigger"
                    f" tsvector_update_{tsvector_name} if it exists before potentially recreating it."
                )
                cursor.execute(f"DROP TRIGGER IF EXISTS tsvector_update_{tsvector_name} ON {qualified_temp_table}")

                self.logger.info(
                    f"Adding tsvector trigger for column {tsvector_name}"
                    f" based on the following columns: {derived_from_cols}"
                )
                derived_from_cols_str = ", ".join(derived_from_cols)
                tsvector_trigger_sql = f"""
                    CREATE TRIGGER tsvector_update_{tsvector_name} BEFORE INSERT OR UPDATE
                    ON {qualified_temp_table} FOR EACH ROW EXECUTE PROCEDURE
                    tsvector_update_trigger({tsvector_name}, '{DEFAULT_TEXT_SEARCH_CONFIG}',
                                            {derived_from_cols_str})
                """
                cursor.execute(tsvector_trigger_sql)
                self.logger.info(f"tsvector trigger for column {tsvector_name} added.")

        return qualified_temp_table

    def _set_sequence_value(self, seq_name: str, val: Optional[int] = None) -> int:
        """
        Used to reset the value of a Postgres sequence. This function should be used for tables that utilize a
        sequence to help ensure we don't cross the threshold of values for an ID field as we reload a table every day.
        If the calling functions exits with an exception this will reset the sequence value to its previous next value.

        Args:
            seq_name: Name of sequence to reset value for
            val: Optional value to reset the sequence to. Defaults to 1

        Returns:
            last_value: the previous value use by the sequence.
        """
        new_seq_val = val if val else 1
        self.logger.info(f"Setting the Postgres sequence to {new_seq_val} for: {seq_name}")
        with db.connection.cursor() as cursor:
            cursor.execute(f"SELECT last_value FROM {seq_name}")
            last_value = cursor.fetchone()[0]
            cursor.execute(f"ALTER SEQUENCE IF EXISTS {seq_name} RESTART WITH {new_seq_val}")
        return last_value

    def _write_to_postgres(
        self,
        spark: SparkSession,
        df: DataFrame,
        qualified_temp_table: str,
        postgres_schema_def: dict,
        delta_column_names: List[str],
        postgres_seq_last_value=None,
    ):
        """
        Writes to Postgres using one of two methods (JDBC inserts or Bulk Copy CSV) based on the `--jdbc-inserts` option
        for this command. When the option is True, this method writes using the JDBC inserts strategy. When False, it
        writes with the Bulk Copy CSV strategy.

        If an error occurs during either, and a previous sequence value for the Postgres table is provided, it will
        reset the sequence value.

        Args:
            spark: the active SparkSession to work within
            df: Dataframe containing data to write to the table
            qualified_temp_table: The fully qualified Postgres table name to write the data to (<schema>.<table_name>)
            postgres_scheme_def: Schema definition for Postgres table being written to in dictionary format. Key value
                pairs contain column name and column types, respectively. Only used for JDBC insert strategy
            delta_column_names: List of Delta columns to read from Dataframe before writing to Postgres.
            postgres_seq_last_value: Last value of the Postgres Sequence. This is provided, so it can be reset to this
                value in the case of an error
        """

        strategy = "JDBC INSERTs" if self.options["jdbc_inserts"] else "SQL bulk COPY CSV"

        self.logger.info(f"LOAD (START): Loading dataframe to {qualified_temp_table} using {strategy} " f"strategy")

        # Make sure that the column order defined in the Delta table schema matches
        # that of the Spark dataframe used to pull from the Postgres table. While not
        # always needed, this should help to prevent any future mismatch between the two.
        df = df.select(delta_column_names)

        try:
            if self.options["jdbc_inserts"]:
                self._write_with_jdbc_inserts(
                    df,
                    qualified_temp_table,
                    postgres_schema_def,
                )
            else:
                self._write_with_sql_bulk_copy_csv(
                    spark,
                    df,
                    delta_s3_path=qualified_temp_table,
                    qualified_temp_table=qualified_temp_table,
                    ordered_col_names=delta_column_names,
                    spark_s3_bucket_name=self.options["spark_s3_bucket"],
                    keep_csv_files=True if self.options["keep_csv_files"] else False,
                )
        except Exception as exc:
            if postgres_seq_last_value:
                self.logger.error(
                    f"Command failed unexpectedly; resetting the sequence to previous value: {postgres_seq_last_value}"
                )
                self._set_sequence_value(self.table_spec["postgres_seq_name"], postgres_seq_last_value)
            raise Exception(exc)

        self.logger.info(f"LOAD (FINISH): Loaded dataframe to {qualified_temp_table} using {strategy} " f"strategy")

    def _write_with_sql_bulk_copy_csv(
        self,
        spark: SparkSession,
        df: DataFrame,
        delta_s3_path: str,
        qualified_temp_table: str,
        ordered_col_names: List[str],
        spark_s3_bucket_name: str,
        keep_csv_files=False,
    ):
        """
        Write-from-delta-to-postgres strategy that relies on SQL bulk COPY of CSV files to Postgres. It uses the SQL
        COPY command on CSV files, which are created from the Delta table's underlying parquet files.

        Since Spark DataFrameWriters for JDBC don't support the COPY command, this custom function streams
        the data from S3 and uses psycopg2 to do the COPY. The file paths of S3 gzipped CSV files to process are
        distributed across the cluster to executors, and the custom function is invoked by each executor,
        using ``rdd.mapPartitionWithIndex``, to then stream the file at that path and send it to Postgres.

        e.g. if there are 100,000 rows to write, across 100 parquet files, being written by a cluster
        of 5 nodes each with 4 cores (yields 4*5 = 20 executors), writing to a DB with max_parallel_workers=16, and the
        ``CONFIG.SPARK_PARTITION_ROWS`` = 500
        - First, 20 executors will work through 100 tasks, where each task is to convert a parquet file to one or
          more CSV files and store it back in S3. 100k rows over 100 parquet files ~1000 rows per file.
          - If maxRecordsPerFile (which is set to ``CONFIG.SPARK_PARTITION_ROWS``) = 500, that yields 2 CSV files for
            every 1 parquet file => 200 CSV files to COPY to Postgres
        - Then for writing to Postgres, the number of connections to have open is calibrated based on the target DB's
          max_parallel_workers.
            - Currently it is one-half of the max_parallel_workers -> 8
        - 8 concurrent JDBC connections will be open by 8 executors; the other 12 will sit idle
        - 8 tasks will be processed by the 8 active executors
        - each task may include a batch of multiple file paths from the RDD (~200/8 = 25 file paths)
        - each active executor will pull 1 file path from its batch in its task, stream the CSV file from S3 to
          Postgres using the SQL COPY command. Each COPY command copies max of ~500 rows in this case
        - once copied, it then moves on to the next file path in its task's batch.
        - once the executor is done with its batch, its task is done, there are no more tasks, it will sit idle
        - which means the DB could be processing 8 COPY commands concurrently

        Args:
            spark: the active SparkSession to work within
            df: the source data, which will be written to CSV files before COPY to Postgres
            delta_s3_path: the unique qualifier of the path in S3 to store temporary CSV files
            qualified_temp_table: the name of the temp table (qualified with schema if needed) in the target Postgres DB
                where the CSV data will be written to with COPY
            ordered_col_names: Ordered list of column names that must match the order of columns in the CSV
                - The DataFrame should have its columns ordered by this
                - And the COPY command should provide these cols so that COPY pulls the right data into the right cols
            keep_csv_files: Whether to prevent overwriting of these temporary CSV files by a subsequent write
                to the same output path. Defaults to False. If True, this will instead put them in a timestamped
                sub-folder of a "temp" folder. Be mindful of cleaning these up if setting to True. If False,
                the same output path is used for each write and nukes-and-paves the files in that output path.
        """
        csv_path = f"{CONFIG.SPARK_CSV_S3_PATH}/{delta_s3_path}/"
        if keep_csv_files:
            csv_path = (
                f"{CONFIG.SPARK_CSV_S3_PATH}/temp/{delta_s3_path}/"
                f"{datetime.strftime(datetime.utcnow(), '%Y%m%d%H%M%S')}/"
            )
        s3_bucket_with_csv_path = f"s3a://{spark_s3_bucket_name}/{csv_path}"

        # Get boto3 s3 resource to interact with bucket where CSV data will land
        if not CONFIG.USE_AWS:
            boto3_session = boto3.session.Session(
                region_name=CONFIG.AWS_REGION,
                aws_access_key_id=CONFIG.AWS_ACCESS_KEY.get_secret_value(),
                aws_secret_access_key=CONFIG.AWS_SECRET_KEY.get_secret_value(),
            )
            s3_resource = boto3_session.resource(
                service_name="s3", region_name=CONFIG.AWS_REGION, endpoint_url=f"http://{CONFIG.AWS_S3_ENDPOINT}"
            )
        else:
            s3_resource = boto3.resource(
                service_name="s3", region_name=CONFIG.AWS_REGION, endpoint_url=f"https://{CONFIG.AWS_S3_ENDPOINT}"
            )
        s3_bucket_name = spark_s3_bucket_name
        s3_bucket = s3_resource.Bucket(s3_bucket_name)
        objs_collection = s3_bucket.objects.filter(Prefix=csv_path)
        initial_size = sum(1 for _ in objs_collection)

        if initial_size > 0:
            self.logger.info(f"LOAD: Starting to delete {initial_size} previous objects in {s3_bucket_with_csv_path}")
            objs_collection.delete()
            post_delete_size = sum(1 for _ in objs_collection)
            self.logger.info(f"LOAD: Finished deleting. {post_delete_size} objects remain in {s3_bucket_with_csv_path}")
        else:
            self.logger.info(f"LOAD: Target S3 path {s3_bucket_with_csv_path} is empty or yet to be created")

        self.logger.info(f"LOAD: Starting dump of Delta table to temp gzipped CSV files in {s3_bucket_with_csv_path}")
        df_no_arrays = convert_array_cols_to_string(df, is_postgres_array_format=True, is_for_csv_export=True)
        df_no_arrays.write.options(
            maxRecordsPerFile=_SPARK_CSV_WRITE_TO_PG_MAX_RECORDS_PER_FILE,
            compression="gzip",
            nullValue=None,
            escape='"',  # " is used to escape the 'quote' character setting (which defaults to "). Escaped quote = ""
            ignoreLeadingWhiteSpace=False,  # must set for CSV write, as it defaults to true
            ignoreTrailingWhiteSpace=False,  # must set for CSV write, as it defaults to true
            timestampFormat=CONFIG.SPARK_CSV_TIMEZONE_FORMAT,
        ).mode(saveMode="overwrite" if not keep_csv_files else "errorifexists").csv(s3_bucket_with_csv_path)

        self.logger.debug(
            f"Connecting to S3 at endpoint_url={CONFIG.AWS_S3_ENDPOINT}, region_name={CONFIG.AWS_REGION} to "
            f"get listing of contents of Bucket={spark_s3_bucket_name} with Prefix={csv_path}"
        )

        gzipped_csv_files = [f.key for f in s3_bucket.objects.filter(Prefix=csv_path) if f.key.endswith(".csv.gz")]
        file_count = len(gzipped_csv_files)
        self.logger.info(f"LOAD: Finished dumping {file_count} CSV files in {s3_bucket_with_csv_path}")

        self.logger.info(
            f"LOAD: Starting SQL bulk COPY of {file_count} CSV files to Postgres {qualified_temp_table} table"
        )

        db_dsn = get_database_dsn_string()
        with psycopg2.connect(dsn=db_dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SHOW max_parallel_workers")
                max_parallel_workers = int(cursor.fetchone()[0])
                # Use the CONFIG.SPARK_CSV_WRITE_TO_PG_PARALLEL_WORKER_MULTIPLIER and
                # CONFIG.SPARK_CSV_WRITE_TO_PG_MIN_PARTITIONS config vars to derive the number of partitions to
                # split/group batches of CSV files into, which need to be written via COPY to PG. This multiplier
                # will be multiplied against the max_parallel_workers value of the target database. It can be a
                # fraction less than 1.0. The final value will be the greater of that or
                # SPARK_CSV_WRITE_TO_PG_MIN_PARTITIONS
                partitions = max(
                    ceil(max_parallel_workers * CONFIG.SPARK_CSV_WRITE_TO_PG_PARALLEL_WORKER_MULTIPLIER),
                    CONFIG.SPARK_CSV_WRITE_TO_PG_MIN_PARTITIONS,
                )

        # Repartition based on DB's configured max_parallel_workers so that there will only be this many concurrent
        # connections writing to Postgres at once, to not overtax it nor oversaturate the number of allowed connections
        # Observations have shown that in production infrastructure, more concurrent connections just lead to I/O
        # throttling
        rdd = spark.sparkContext.parallelize(gzipped_csv_files, partitions)

        # WARNING: rdd.map needs to use cloudpickle to pickle the mapped function, its arguments, and in-turn any
        # imported dependencies from either of those two as well as from the module from which the function is
        # imported. If at any point a new transitive dependency is introduced
        # into the mapped function, its module, or an arg of it ... that is not pickle-able, this will throw an error.
        # One way to help is to resolve all arguments to primitive types (int, string) that can be passed
        # to the mapped function
        rdd.mapPartitionsWithIndex(
            lambda partition_idx, s3_obj_keys: copy_csvs_from_s3_to_pg(
                batch_num=partition_idx,
                s3_bucket_name=s3_bucket_name,
                s3_obj_keys=s3_obj_keys,
                db_dsn=db_dsn,
                target_pg_table=qualified_temp_table,
                ordered_col_names=ordered_col_names,
                gzipped=True,
                work_mem_override=_PG_WORK_MEM_FOR_LARGE_CSV_COPY,
            ),
        ).collect()

        self.logger.info(
            f"LOAD: Finished SQL bulk COPY of {file_count} CSV files to Postgres {qualified_temp_table} table"
        )

    def _write_with_jdbc_inserts(
        self,
        df: DataFrame,
        qualified_temp_table: str,
        postgres_schema_def: Dict,
    ):
        """
        Write-from-delta-to-postgres strategy that leverages the native Spark ``DataFrame.write.jdbc`` approach.
        This will issue a series of individual INSERT statements over a JDBC connection-per-executor.
        e.g. if there are 100,000 rows to write, across 100 parquet files, being written by a cluster
        of 5 nodes each with 4 cores (yields 4*5 = 20 executors)
        - 20 concurrent JDBC connections will be open for the 20 executors
        - 100 tasks will be processed by the executors
        - each executor will handle 1 task = 1 file = ~1000 rows in 1 file
        - each executor will issue 1000 INSERT statements, and then move on to the next file
        - which means the DB could be processing 20*1000 = 20,000 INSERT statements concurrently

        Args:
            df: the source data, which will be written to CSV files before COPY to Postgres
            qualified_temp_table: the name of the temp table (qualified with schema if needed) in the target Postgres DB
                where the CSV data will be written to with COPY
            postgres_schema_def: Schema of table being written to
        """
        special_columns = {}
        col_type_mapping = list(postgres_schema_def.items())

        # We are taking control of destination table creation, and not letting Spark auto-create it based
        # on inference from the source DataFrame's schema, there could be incompatible col data types that need
        # special handling. Get those columns and handle each.
        for column_name, column_type in col_type_mapping:
            if column_type in SPECIAL_TYPES_MAPPING:
                special_columns[column_name] = column_type
        self.logger.info(f"Special Columns: {special_columns}")
        split_dfs = self._split_dfs(df, list(special_columns))
        split_df_count = len(split_dfs)

        for i, split_df in enumerate(split_dfs):
            # Note: we're only appending here as we don't want to re-truncate or overwrite with multiple dataframes
            self.logger.info(f"LOAD: Loading part {i + 1} of {split_df_count} (note: unequal part sizes)")
            split_df.write.jdbc(
                url=get_usas_jdbc_url(),
                table=qualified_temp_table,
                mode="append",
                properties=get_jdbc_connection_properties(),
            )
            self.logger.info(f"LOAD: Part {i + 1} of {split_df_count} loaded (note: unequal part sizes)")
