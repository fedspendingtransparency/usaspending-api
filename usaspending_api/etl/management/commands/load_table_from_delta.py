import itertools
import numpy as np
from django import db
from django.core.management.base import BaseCommand
from pyspark.sql import SparkSession

from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
    get_jdbc_connection_properties,
    get_jdbc_url,
    get_jvm_logger,
)
from usaspending_api.etl.management.commands.create_delta_table import TABLE_SPEC
from usaspending_api.database_scripts.matview_generator.chunked_matview_sql_generator import (
    make_copy_indexes,
    make_copy_constraints,
)

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
    exists, all existing data will be deleted before new data is written.
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
            "--recreate",
            action="store_true",
            help="Instead of truncating and reloading into an existing table, this forces the script to drop and"
            "rebuild the table from scratch.",
        )

    # Unfortunately, pySpark with the JDBC doesn't handle UUIDs/JSON well.
    # In addition to using "stringype": "unspecified", it can't handle null values in the UUID columns
    # The only way to get around this is to split the dataframe into smaller chunks filtering out the null values.
    # In said smaller chunks, where it would be null, we simply drop the column entirely from the insert,
    # resorting to the default null values.
    def _split_dfs(self, df, special_columns):
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
            # See comment below about old date and time values cannot parsed without these
            "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
            "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
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
        create = False

        table_spec = TABLE_SPEC[delta_table]
        special_columns = {}

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

        # Resolve JDBC URL for Source Database
        jdbc_url = get_jdbc_url()
        if not jdbc_url:
            raise RuntimeError(f"Couldn't find JDBC url, please properly configure your CONFIG.")
        if not jdbc_url.startswith("jdbc:postgresql://"):
            raise ValueError("JDBC URL given is not in postgres JDBC URL format (e.g. jdbc:postgresql://...")

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
        # Recreate the table if it doesn't exist. Spark's df.write automatically does this but doesn't account for
        # the extra metadata (indexes, constraints, defaults) which CREATE TABLE X LIKE Y accounts for.
        # If there is no postgres_table to base it on, it just relies on spark to make it and work with delta table
        if make_new_table and (postgres_table or postgres_cols):
            if postgres_table:
                create_temp_sql = f"""
                    CREATE TABLE {temp_table} (
                        LIKE {postgres_table} INCLUDING DEFAULTS INCLUDING IDENTITY
                    )
                """
            else:
                create_temp_sql = f"""
                    CREATE TABLE {temp_table} (
                        {", ".join([f'{key} {val}' for key, val in postgres_cols.items()])}
                    )
                """
            with db.connection.cursor() as cursor:
                logger.info(f"Creating {temp_table}")
                cursor.execute(create_temp_sql)
                logger.info(f"{temp_table} created.")

                # Copy over the constraints before indexes
                # Note: we could of included constraints above (`INCLUDING CONSTRAINTS`) but we need to drop the
                #       foreign key ones
                if postgres_table:
                    logger.info(f"Copying constraints over from {postgres_table}")
                    copy_constraint_sql = make_copy_constraints(
                        cursor, postgres_table, temp_table, drop_foreign_keys=True
                    )
                    if copy_constraint_sql:
                        cursor.execute("; ".join(copy_constraint_sql))
                    logger.info(f"Constraints from {postgres_table} copied over")

            # Getting a list of the special columns that will need to be particularly handed
            if postgres_model:
                postgres_cols = [(column.name, type(column)) for column in postgres_model._meta.get_fields()]
            else:
                postgres_cols = list(postgres_cols.items())
            for column_name, column_type in postgres_cols:
                if column_type in SPECIAL_TYPES_MAPPING:
                    special_columns[column_name] = column_type

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
        logger.info(f"LOAD (START): Loading data from Delta table {delta_table} to {temp_table}")
        split_dfs = self._split_dfs(df, list(special_columns))
        split_df_count = len(split_dfs)
        for i, split_df in enumerate(split_dfs):
            # Note: we're only appending here as we don't want to re-truncate or overwrite with multiple dataframes
            logger.info(f"LOAD: Loading part {i+1} of {split_df_count} (note: unequal part sizes)")
            split_df.write.jdbc(
                url=jdbc_url,
                table=temp_table,
                mode="append",
                properties=get_jdbc_connection_properties(boost_writing=True),
            )
            logger.info(f"LOAD: Part {i+1} of {split_df_count} loaded (note: unequal part sizes)")
        logger.info(f"LOAD (FINISH): Loaded data from Delta table {delta_table} to {temp_table}")

        # Load indexes if applicable
        if make_new_table and postgres_table:
            with db.connection.cursor() as cursor:
                # Ensuring we're using the max cores available when generating indexes
                # TODO: dynamically set rds_core_count by a setting per environment.
                copy_index_sql = []
                rds_core_count = 8
                index_performance_sql = f"SET max_parallel_maintenance_workers = {rds_core_count}"
                copy_index_sql.append(index_performance_sql)

                # Copy over the indexes, preserving the names (mostly, includes "_temp")
                # Note: We could of included indexes above (`INCLUDING INDEXES`) but that renames them,
                #       which would run into issues with migrations that have specific names.
                #       Additionally, we want to run this after we loaded in the data for performance.
                copy_index_sql.extend(make_copy_indexes(cursor, postgres_table, temp_table))

                if copy_index_sql:
                    logger.info(f"Copying indexes over from {postgres_table}.")
                    cursor.execute("; ".join(copy_index_sql))
                    logger.info(f"Indexes from {postgres_table} copied over.")

        if spark_created_by_command:
            spark.stop()
