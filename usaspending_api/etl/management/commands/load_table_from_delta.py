import itertools

import boto3
import numpy as np
from django import db
from django.core.management.base import BaseCommand
from django.db.models import Model
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Optional
from datetime import datetime

from usaspending_api.common.csv_helpers import copy_csv_from_s3_to_pg
from usaspending_api.common.etl.spark import convert_array_cols_to_string
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
    get_jdbc_connection_properties,
    get_jdbc_url,
    get_jvm_logger,
)
from usaspending_api.config import CONFIG
from usaspending_api.etl.management.commands.create_delta_table import TABLE_SPEC

# Note: the `delta` type is not actually in Spark SQL. It's how we're temporarily storing the data before converting it
#       to the proper postgres type, since pySpark doesn't automatically support this conversion.
SPECIAL_TYPES_MAPPING = {
    db.models.UUIDField: {"postgres": "UUID USING {column_name}::UUID", "delta": "TEXT"},
    "UUID": {"postgres": "UUID USING {column_name}::UUID", "delta": "TEXT"},
    db.models.JSONField: {"postgres": "JSONB using {column_name}::JSON", "delta": "TEXT"},
    "JSONB": {"postgres": "JSONB using {column_name}::JSON", "delta": "TEXT"},
}


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
        logger = get_jvm_logger(spark)

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
        if postgres_table_name:
            postgres_table = f"{postgres_schema}.{postgres_table_name}" if postgres_schema else postgres_table_name

        # Postgres side - temp
        temp_schema = "temp"
        if postgres_table:
            temp_table_name = f"{postgres_table_name}_temp"
        else:
            temp_table_name = f"{delta_table_name}_temp"
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

        # If it does and we're recreating it, drop it first
        if temp_dest_table_exists and recreate:
            logger.info(f"{temp_table} exists and recreate argument provided. Dropping first.")
            # If the schema has changed and we need to do a complete reload, just drop the table and rebuild it
            clear_table_sql = f"DROP TABLE {temp_table}"
            with db.connection.cursor() as cursor:
                cursor.execute(clear_table_sql)
            logger.info(f"{temp_table} dropped.")
            temp_dest_table_exists = False
        make_new_table = not temp_dest_table_exists

        if postgres_table or postgres_cols:
            # Recreate the table if it doesn't exist. Spark's df.write automatically does this but doesn't account for
            # the extra metadata (indexes, constraints, defaults) which CREATE TABLE X LIKE Y accounts for.
            # If there is no postgres_table to base it on, it just relies on spark to make it and work with delta table
            if make_new_table:
                if postgres_table:
                    create_temp_sql = f"""
                        CREATE TABLE {temp_table} (
                            LIKE {postgres_table} INCLUDING DEFAULTS INCLUDING IDENTITY
                        ) WITH (autovacuum_enabled=FALSE)
                    """
                elif postgres_cols:
                    create_temp_sql = f"""
                        CREATE TABLE {temp_table} (
                            {", ".join([f'{key} {val}' for key, val in postgres_cols.items()])}
                        ) WITH (autovacuum_enabled=FALSE)
                    """
                with db.connection.cursor() as cursor:
                    logger.info(f"Creating {temp_table}")
                    cursor.execute(create_temp_sql)
                    logger.info(f"{temp_table} created.")

        # Read from Delta
        df = spark.table(delta_table)

        # Make sure that the column order defined in the Delta table schema matches
        # that of the Spark dataframe used to pull from the Postgres table. While not
        # always needed, this should help to prevent any future mismatch between the two.
        if table_spec.get("column_names"):
            df = df.select(table_spec.get("column_names"))

        # If we're working off an existing table, truncate before loading in all the data
        if not make_new_table:
            with db.connection.cursor() as cursor:
                cursor.execute(f"TRUNCATE {temp_table}")

        # Write to Postgres
        use_jdbc_inserts = options["jdbc_inserts"]
        strategy = "JDBC INSERTs" if use_jdbc_inserts else "SQL bulk COPY CSV"
        logger.info(f"LOAD (START): Loading data from Delta table {delta_table} to {temp_table} using {strategy} "
                    f"strategy")

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
            self._write_with_sql_bulk_copy_csv(

            )

        logger.info(f"LOAD (FINISH): Loaded data from Delta table {delta_table} to {temp_table} using {strategy} "
                    f"strategy")

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

    def _write_with_sql_bulk_copy_csv(
        self,
        spark: SparkSession,
        df: DataFrame,
        delta_db: str,
        delta_table: str,
        temp_table: str,
    ):
        logger = get_jvm_logger(spark)
        csv_path = f"{CONFIG.SPARK_CSV_S3_PATH}/temp/{delta_db}/{delta_table}/{datetime.strftime(datetime.utcnow(), '%Y%m%d%H%M%S')}/"
        s3_bucket_with_csv_path = f"s3a://{CONFIG.SPARK_S3_BUCKET}/{csv_path}"
        logger.info(f"LOAD: Starting dump of Delta table to temp gzipped CSV files in {s3_bucket_with_csv_path}")
        df_no_arrays = convert_array_cols_to_string(df)
        df_no_arrays.write.options(
            compression="gzip",
            nullValue=None,
            escape='"',
        ).csv(s3_bucket_with_csv_path)

        s3_resource = boto3.resource("s3", region_name=CONFIG.AWS_REGION)
        s3_bucket = s3_resource.Bucket(CONFIG.SPARK_S3_BUCKET)
        gzipped_csv_files = [f.key for f in s3_bucket.objects.filter(Prefix=csv_path) if f.key.endswith(".csv.gz")]
        file_count = len(gzipped_csv_files)
        logger.info(f"LOAD: Finished dumping {file_count} CSV files in {s3_bucket_with_csv_path}")

        logger.info(f"LOAD: Starting bulk COPY of {file_count} CSV files to Postgres {temp_table} table")
        rdd = spark.sparkContext.parallelize(gzipped_csv_files)
        results = rdd.map(
            lambda s3_obj_key: copy_csv_from_s3_to_pg(
                s3_bucket=s3_bucket,
                s3_obj_key=s3_obj_key,
                target_pg_table=temp_table,
                gzipped=True,
                logger=logger,
            )
        ).collect()
        logger.info(f"LOAD: Finished bulk COPY of {file_count} CSV files to Postgres {temp_table} table")

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
        logger = get_jvm_logger(spark)
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
                raise RuntimeError("Multiple DataFrame subsets need to be appended to the destination "
                                   "table back-to-back but the write was set to overwrite, which is incorrect.")
            for i, split_df in enumerate(split_dfs):
                # Note: we're only appending here as we don't want to re-truncate or overwrite with multiple dataframes
                logger.info(f"LOAD: Loading part {i + 1} of {split_df_count} (note: unequal part sizes)")
                split_df.write.jdbc(
                    url=get_jdbc_url(),
                    table=temp_table,
                    mode=save_mode,
                    properties=get_jdbc_connection_properties(),
                )
                logger.info(f"LOAD: Part {i + 1} of {split_df_count} loaded (note: unequal part sizes)")
        else:
            # Do it in one shot
            df.write.jdbc(
                url=get_jdbc_url(),
                table=temp_table,
                mode=save_mode,
                properties=get_jdbc_connection_properties(),
            )
