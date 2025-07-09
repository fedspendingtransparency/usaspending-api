import itertools
import logging

import boto3
import numpy as np
import psycopg2

from django import db
from django.core.management.base import BaseCommand
from django.db.models import Model
from math import ceil
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Optional, List
from datetime import datetime

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

from usaspending_api.etl.management.commands.create_delta_table import TABLE_SPEC

logger = logging.getLogger(__name__)

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
            "--recreate",
            action="store_true",
            help="Instead of truncating and reloading into an existing table, this forces the script to drop and"
            "rebuild the table from scratch.",
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
            "If the job fails for some unexpected reason then the sequence will be reset to the previous value.",
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

        # Resolve Parameters
        delta_table = options["delta_table"]
        recreate = options["recreate"]

        table_spec = TABLE_SPEC[delta_table]

        # Delta side
        destination_database = options["alt_delta_db"] or table_spec["destination_database"]
        delta_table_name = options["alt_delta_name"] or delta_table
        delta_table = f"{destination_database}.{delta_table_name}" if destination_database else delta_table_name

        # Postgres side - source
        postgres_table = None
        postgres_model = table_spec["model"]
        postgres_schema = table_spec["source_database"] or table_spec["swap_schema"]
        postgres_table_name = table_spec["source_table"] or table_spec["swap_table"]
        postgres_cols = table_spec["source_schema"]
        column_names = table_spec.get("column_names")
        tsvectors = table_spec.get("tsvectors") or {}
        if postgres_table_name:
            postgres_table = f"{postgres_schema}.{postgres_table_name}" if postgres_schema else postgres_table_name

        # Postgres side - temp
        temp_schema = "temp"
        temp_table_suffix = "temp"
        temp_table_suffix_appendage = f"_{temp_table_suffix}" if {temp_table_suffix} else ""
        if postgres_table:
            temp_table_name = f"{postgres_table_name}{temp_table_suffix_appendage}"
        else:
            temp_table_name = f"{delta_table_name}{temp_table_suffix_appendage}"
        temp_table = f"{temp_schema}.{temp_table_name}"

        summary_msg = f"Copying delta table {delta_table} to a Postgres temp table {temp_table}."
        if postgres_table:
            summary_msg = f"{summary_msg} The temp table will be based on the postgres table {postgres_table}"
        logger.info(summary_msg)

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

        # If it does, and we're recreating it, drop it first
        if temp_dest_table_exists and recreate:
            logger.info(f"{temp_table} exists and recreate argument provided. Dropping first.")
            # If the schema has changed and we need to do a complete reload, just drop the table and rebuild it
            clear_table_sql = f"DROP TABLE {temp_table}"
            with db.connection.cursor() as cursor:
                cursor.execute(clear_table_sql)
            logger.info(f"{temp_table} dropped.")
            temp_dest_table_exists = False
        make_new_table = not temp_dest_table_exists

        is_postgres_table_partitioned = table_spec.get("postgres_partition_spec") is not None

        if postgres_table or postgres_cols:
            # Recreate the table if it doesn't exist. Spark's df.write automatically does this but doesn't account for
            # the extra metadata (indexes, constraints, defaults) which CREATE TABLE X LIKE Y accounts for.
            # If there is no postgres_table to base it on, it just relies on spark to make it and work with delta table
            if make_new_table:
                partition_clause = ""
                storage_parameters = "WITH (autovacuum_enabled=FALSE)"
                partitions_sql = []
                if is_postgres_table_partitioned:
                    partition_clause = (
                        f"PARTITION BY {table_spec['postgres_partition_spec']['partitioning_form']}"
                        f"({', '.join(table_spec['postgres_partition_spec']['partition_keys'])})"
                    )
                    storage_parameters = ""
                    partitions_sql = [
                        (
                            f"CREATE TABLE "
                            # Below: e.g. my_tbl_temp -> my_tbl_part_temp
                            f"{temp_table[:-len(temp_table_suffix_appendage)]}{pt['table_suffix']}{temp_table_suffix_appendage} "
                            f"PARTITION OF {temp_table} {pt['partitioning_clause']} "
                            f"{storage_parameters}"
                        )
                        for pt in table_spec["postgres_partition_spec"]["partitions"]
                    ]
                if postgres_table:
                    create_temp_sql = f"""
                        CREATE TABLE {temp_table} (
                            LIKE {postgres_table} INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING IDENTITY
                        ) {partition_clause} {storage_parameters}
                    """
                elif postgres_cols:
                    create_temp_sql = f"""
                        CREATE TABLE {temp_table} (
                            {", ".join([f'{key} {val}' for key, val in postgres_cols.items()])}
                        ) {partition_clause} {storage_parameters}
                    """
                else:
                    raise RuntimeError(
                        "make_new_table=True but neither a postgres_table or postgres_cols are "
                        "populated for the target delta table in the TABLE_SPEC"
                    )
                with db.connection.cursor() as cursor:
                    logger.info(f"Creating {temp_table}")
                    cursor.execute(create_temp_sql)
                    logger.info(f"{temp_table} created.")

                    if is_postgres_table_partitioned and partitions_sql:
                        for create_partition in partitions_sql:
                            logger.info(f"Creating partition of {temp_table} with SQL:\n{create_partition}")
                            cursor.execute(create_partition)
                            logger.info("Partition created.")

                    # If there are vectors, add the triggers that will populate them based on other calls
                    # NOTE: Undetermined whether tsvector triggers can be applied on partitioned tables,
                    #       at the top-level virtual/partitioned table (versus having to apply on each partition)
                    for tsvector_name, derived_from_cols in tsvectors.items():
                        logger.info(
                            f"To prevent any confusion or duplicates, dropping the trigger"
                            f" tsvector_update_{tsvector_name} if it exists before potentially recreating it."
                        )
                        cursor.execute(f"DROP TRIGGER IF EXISTS tsvector_update_{tsvector_name} ON {temp_table}")

                        logger.info(
                            f"Adding tsvector trigger for column {tsvector_name}"
                            f" based on the following columns: {derived_from_cols}"
                        )
                        derived_from_cols_str = ", ".join(derived_from_cols)
                        tsvector_trigger_sql = f"""
                            CREATE TRIGGER tsvector_update_{tsvector_name} BEFORE INSERT OR UPDATE
                            ON {temp_table} FOR EACH ROW EXECUTE PROCEDURE
                            tsvector_update_trigger({tsvector_name}, '{DEFAULT_TEXT_SEARCH_CONFIG}',
                                                    {derived_from_cols_str})
                        """
                        cursor.execute(tsvector_trigger_sql)
                        logger.info(f"tsvector trigger for column {tsvector_name} added.")

        # Read from Delta
        df = spark.table(delta_table)

        # Make sure that the column order defined in the Delta table schema matches
        # that of the Spark dataframe used to pull from the Postgres table. While not
        # always needed, this should help to prevent any future mismatch between the two.
        if column_names:
            df = df.select(column_names)

        # If we're working off an existing table, truncate before loading in all the data
        if not make_new_table:
            logger.info(f"Truncating existing table {temp_table}")
            with db.connection.cursor() as cursor:
                cursor.execute(f"TRUNCATE {temp_table}")
                logger.info(f"{temp_table} truncated.")

        # Reset the sequence before load for a table if it exists
        if options["reset_sequence"] and table_spec.get("postgres_seq_name"):
            postgres_seq_last_value = self._set_sequence_value(table_spec["postgres_seq_name"])
        else:
            postgres_seq_last_value = None

        # Write to Postgres
        use_jdbc_inserts = options["jdbc_inserts"]
        strategy = "JDBC INSERTs" if use_jdbc_inserts else "SQL bulk COPY CSV"
        logger.info(
            f"LOAD (START): Loading data from Delta table {delta_table} to {temp_table} using {strategy} " f"strategy"
        )

        try:
            if use_jdbc_inserts:
                self._write_with_jdbc_inserts(
                    spark,
                    df,
                    temp_table,
                    split_df_by_special_cols=True,
                    postgres_model=postgres_model,
                    postgres_cols=postgres_cols,
                    overwrite=False,
                )
            else:
                if not column_names:
                    raise RuntimeError("column_names None or empty, but are required to map CSV cols to table cols")
                spark_s3_bucket_name = options["spark_s3_bucket"]
                self._write_with_sql_bulk_copy_csv(
                    spark,
                    df,
                    delta_db=destination_database,
                    delta_table_name=delta_table_name,
                    temp_table=temp_table,
                    ordered_col_names=column_names,
                    spark_s3_bucket_name=spark_s3_bucket_name,
                    keep_csv_files=True if options["keep_csv_files"] else False,
                )
        except Exception as exc:
            if postgres_seq_last_value:
                logger.error(
                    f"Command failed unexpectedly; resetting the sequence to previous value: {postgres_seq_last_value}"
                )
                self._set_sequence_value(table_spec["postgres_seq_name"], postgres_seq_last_value)
            raise Exception(exc)

        logger.info(
            f"LOAD (FINISH): Loaded data from Delta table {delta_table} to {temp_table} using {strategy} " f"strategy"
        )

        # We're done with spark at this point
        if spark_created_by_command:
            spark.stop()

        if postgres_table:
            logger.info(
                f"Note: this has merely loaded the data from Delta. For various reasons, we've separated the"
                f" metadata portion of the table download to a separate script. If not already done so,"
                f" please run the following additional command to complete the process: "
                f" 'copy_table_metadata --source-table {postgres_table} --dest-table {temp_table}'."
            )

    def _set_sequence_value(self, seq_name: str, val: Optional[int] = None) -> int:
        """
        Used to reset the value of a Postgres sequence. This function should be used for tables that utilize a
        sequence to help ensure we don't cross the threshold of values for an ID field as we reload a table every day.
        If the calling functions exits with an exception this will reset the sequence value to its previous next value.

        Returns the previous value use by the sequence.
        """
        new_seq_val = val if val else 1
        logger.info(f"Setting the Postgres sequence to {new_seq_val} for: {seq_name}")
        with db.connection.cursor() as cursor:
            cursor.execute(f"SELECT last_value FROM {seq_name}")
            last_value = cursor.fetchone()[0]
            cursor.execute(f"ALTER SEQUENCE IF EXISTS {seq_name} RESTART WITH {new_seq_val}")
        return last_value

    def _write_with_sql_bulk_copy_csv(
        self,
        spark: SparkSession,
        df: DataFrame,
        delta_db: str,
        delta_table_name: str,
        temp_table: str,
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
            spark (SparkSession): the active SparkSession to work within
            df (DataFrame): the source data, which will be written to CSV files before COPY to Postgres
            delta_db (str): the Delta Lake database in which to find the Delta table as source of the DataFrame
            delta_table_name (str): the Delta table used as source of the DataFrame
            temp_table (str): the name of the temp table (qualified with schema if needed) in the target Postgres DB
                where the CSV data will be written to with COPY
            ordered_col_names (List[str]): Ordered list of column names that must match the order of columns in the CSV
                - The DataFrame should have its columns ordered by this
                - And the COPY command should provide these cols so that COPY pulls the right data into the right cols
            keep_csv_files (bool): Whether to prevent overwriting of these temporary CSV files by a subsequent write
                to the same output path. Defaults to False. If True, this will instead put them in a timestamped
                sub-folder of a "temp" folder. Be mindful of cleaning these up if setting to True. If False,
                the same output path is used for each write and nukes-and-paves the files in that output path.
        """
        csv_path = f"{CONFIG.SPARK_CSV_S3_PATH}/{delta_db}/{delta_table_name}/"
        if keep_csv_files:
            csv_path = (
                f"{CONFIG.SPARK_CSV_S3_PATH}/temp/{delta_db}/{delta_table_name}/"
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
            logger.info(f"LOAD: Starting to delete {initial_size} previous objects in {s3_bucket_with_csv_path}")
            objs_collection.delete()
            post_delete_size = sum(1 for _ in objs_collection)
            logger.info(f"LOAD: Finished deleting. {post_delete_size} objects remain in {s3_bucket_with_csv_path}")
        else:
            logger.info(f"LOAD: Target S3 path {s3_bucket_with_csv_path} is empty or yet to be created")

        logger.info(f"LOAD: Starting dump of Delta table to temp gzipped CSV files in {s3_bucket_with_csv_path}")
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

        logger.debug(
            f"Connecting to S3 at endpoint_url={CONFIG.AWS_S3_ENDPOINT}, region_name={CONFIG.AWS_REGION} to "
            f"get listing of contents of Bucket={spark_s3_bucket_name} with Prefix={csv_path}"
        )

        gzipped_csv_files = [f.key for f in s3_bucket.objects.filter(Prefix=csv_path) if f.key.endswith(".csv.gz")]
        file_count = len(gzipped_csv_files)
        logger.info(f"LOAD: Finished dumping {file_count} CSV files in {s3_bucket_with_csv_path}")

        logger.info(f"LOAD: Starting SQL bulk COPY of {file_count} CSV files to Postgres {temp_table} table")

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
                target_pg_table=temp_table,
                ordered_col_names=ordered_col_names,
                gzipped=True,
                work_mem_override=_PG_WORK_MEM_FOR_LARGE_CSV_COPY,
            ),
        ).collect()

        logger.info(f"LOAD: Finished SQL bulk COPY of {file_count} CSV files to Postgres {temp_table} table")

    def _write_with_jdbc_inserts(
        self,
        spark: SparkSession,
        df: DataFrame,
        temp_table: str,
        split_df_by_special_cols: bool = False,
        postgres_model: Optional[Model] = None,
        postgres_cols: Optional[Dict[str, str]] = None,
        overwrite: bool = False,
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
            spark (SparkSession): the active SparkSession to work within
            df (DataFrame): the source data, which will be written to CSV files before COPY to Postgres
            temp_table (str): the name of the temp table (qualified with schema if needed) in the target Postgres DB
                where the CSV data will be written to with COPY
            split_df_by_special_cols (bool): Whether the data provided in the DataFrame is known to have special
                columns for which there is not a data type in Spark but there is in
                Postgres (like JSON or UUID datatype), and so they need to be handled specially in disjoint subsets
                of the DataFrame. Defaults to False
            postgres_model (Optional[Model]): Django Model object for the target Postgres table
            postgres_cols Optional[Dict[str, str]]): Mapping of target table col names to Postgres data type
            overwrite (bool): Defaults to False. Controls whether the table should be truncated first before
            INSERTing (if True), or if INSERTs should append on existing data in the table (if False). NOTE: The table
            may already have been TRUNCATEd as part of the setup of this job before it gets to this step of writing
            to it.
        """
        special_columns = {}
        save_mode = "overwrite" if overwrite else "append"

        # If we are taking control of destination table creation, and not letting Spark auto-create it based
        # on inference from the source DataFrame's schema, there could be incompatible col data types that need
        # special handling. Get those columns and handle each.
        if split_df_by_special_cols:
            if postgres_model:
                col_type_mapping = [(column.name, type(column)) for column in postgres_model._meta.get_fields()]
            else:
                col_type_mapping = list(postgres_cols.items())
            for column_name, column_type in col_type_mapping:
                if column_type in SPECIAL_TYPES_MAPPING:
                    special_columns[column_name] = column_type
            split_dfs = self._split_dfs(df, list(special_columns))
            split_df_count = len(split_dfs)
            if split_df_count > 1 and save_mode != "append":
                raise RuntimeError(
                    "Multiple DataFrame subsets need to be appended to the destination "
                    "table back-to-back but the write was set to overwrite, which is incorrect."
                )
            for i, split_df in enumerate(split_dfs):
                # Note: we're only appending here as we don't want to re-truncate or overwrite with multiple dataframes
                logger.info(f"LOAD: Loading part {i + 1} of {split_df_count} (note: unequal part sizes)")
                split_df.write.jdbc(
                    url=get_usas_jdbc_url(),
                    table=temp_table,
                    mode=save_mode,
                    properties=get_jdbc_connection_properties(),
                )
                logger.info(f"LOAD: Part {i + 1} of {split_df_count} loaded (note: unequal part sizes)")
        else:
            # Do it in one shot
            df.write.jdbc(
                url=get_usas_jdbc_url(),
                table=temp_table,
                mode=save_mode,
                properties=get_jdbc_connection_properties(),
            )
