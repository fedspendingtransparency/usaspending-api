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

SPECIAL_TYPES_MAPPING = {
    db.models.UUIDField: {"postgres": "UUID USING {column_name}::UUID", "delta": "TEXT"},
    "UUID": {"postgres": "UUID USING {column_name}::UUID", "delta": "TEXT"},
    db.models.JSONField: {"postgres": "JSONB using {column_name}::JSON", "delta": "TEXT"},
    "JSONB": {"postgres": "JSONB using {column_name}::JSON", "delta": "TEXT"},
    db.models.DateTimeField: {"postgres": "TIMESTAMP WITH TIME ZONE", "delta": "TIMESTAMP WITHOUT TIME ZONE"},
    "TIMESTAMP": {"postgres": "TIMESTAMP WITH TIME ZONE", "delta": "TIMESTAMP WITHOUT TIME ZONE"},
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
            help="An alternate delta table name to load, overriding the TABLE_SPEC destination_table"
            "name",
        )

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

        table_spec = TABLE_SPEC[delta_table]
        custom_schema = table_spec["custom_schema"]
        special_columns = {}

        # Delta side
        destination_database = options["alt_delta_db"] or table_spec["destination_database"]
        delta_table_name = options["alt_delta_name"] or delta_table
        delta_table = f"{destination_database}.{delta_table_name}" if destination_database else delta_table_name

        # Postgres side
        source_table = None
        source_model = table_spec["model"]
        source_database = table_spec["source_database"]
        source_table_name = table_spec["source_table"]
        source_schema = table_spec["source_schema"]
        if source_table_name:
            source_table = f"{source_database}.{source_table_name}" if source_database else source_table_name

        # Temp side
        temp_schema = "temp"
        if source_table:
            temp_destination_table_name = f"{source_table_name}_temp"
        else:
            temp_destination_table_name = f"{delta_table_name}_temp"
        temp_destination_table = f"{temp_schema}.{temp_destination_table_name}"

        summary_msg = f"Copying delta table{delta_table} to a Postgres temp table {temp_destination_table}."
        if source_table:
            summary_msg = f"{summary_msg} The temp table will be based on the postgres table {source_table}"
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
                    AND table_name = '{temp_destination_table_name}')
        """
        with db.connection.cursor() as cursor:
            cursor.execute(temp_dest_table_exists_sql)
            temp_dest_table_exists = cursor.fetchone()[0]

        # If it does and we're recreating it, drop it first
        if temp_dest_table_exists:
            logger.info(f"{temp_destination_table} exists and recreate argument provided. Dropping first.")
            # If the schema has changed and we need to do a complete reload, just drop the table and rebuild it
            clear_table_sql = f"DROP TABLE {temp_destination_table}"
            with db.connection.cursor() as cursor:
                cursor.execute(clear_table_sql)
            logger.info(f"{temp_destination_table} dropped.")

        # Recreate the table if it doesn't exist. Spark's df.write automatically does this but doesn't account for
        # the extra stuff (indexes, constraints, defaults) which CREATE TABLE X LIKE Y accounts for.
        # If there is no source_table to base it on, it just relies on spark to make it and work with delta table
        if source_table or source_schema:
            if source_table:
                create_temp_sql = f"""
                    CREATE TABLE {temp_destination_table} (
                        LIKE {source_table} INCLUDING DEFAULTS INCLUDING IDENTITY
                    )
                """
            else:
                create_temp_sql = f"""
                    CREATE TABLE {temp_destination_table} (
                        {", ".join([f'{key} {val}' for key, val in source_schema.items()])}
                    )
                """
            with db.connection.cursor() as cursor:
                logger.info(f"Creating {temp_destination_table}")
                cursor.execute(create_temp_sql)
                logger.info(f"{temp_destination_table} created.")

                # Copy over the constraints before indexes
                # Note: we could of included indexes above (`INCLUDING CONSTRAINTS`) but we need to drop the
                #       foreign key ones
                if source_table:
                    logger.info(f"Copying constraints over from {source_table}")
                    copy_constraint_sql = make_copy_constraints(
                        cursor, source_table, temp_destination_table, drop_foreign_keys=True
                    )
                    if copy_constraint_sql:
                        cursor.execute("; ".join(copy_constraint_sql))
                    logger.info(f"Constraints from {source_table} copied over")

            # Converting unsupported data types on spark to their delta equivalent
            # We will cast these back to their original intended types afterwards
            # Only applicable if we're copying down a table that was already in postgres
            alter_column_sql = []
            source_columns = []
            if source_model:
                source_columns = [(column.name, type(column)) for column in source_model._meta.get_fields()]
            else:
                source_columns = list(source_schema.items())

            for column_name, column_type in source_columns:
                if column_type in SPECIAL_TYPES_MAPPING:
                    special_columns[column_name] = column_type
                    delta_type = SPECIAL_TYPES_MAPPING[column_type]["delta"].format(column_name=column_name)
                    alter_column_sql.append(
                        f"ALTER TABLE {temp_destination_table}" f" ALTER COLUMN {column_name} TYPE {delta_type}"
                    )
            if alter_column_sql:
                with db.connection.cursor() as cursor:
                    cursor.execute(";".join(alter_column_sql))

        # Read from Delta
        df = spark.table(delta_table)

        # Make sure that the column order defined in the Delta table schema matches
        # that of the Spark dataframe used to pull from the Postgres table. While not
        # always needed, this should help to prevent any future mismatch between the two.
        if table_spec.get("column_names"):
            df = df.select(table_spec.get("column_names"))

        # Write to Postgres
        logger.info(f"LOAD (START): Loading data from Delta table {delta_table} to {temp_destination_table}")
        df.write.options(customSchema=custom_schema, truncate=True).jdbc(
            url=jdbc_url, table=temp_destination_table, mode="overwrite", properties=get_jdbc_connection_properties()
        )
        logger.info(f"LOAD (FINISH): Loaded data from Delta table {delta_table} to {temp_destination_table}")

        if source_table or source_schema:
            # Recast the special case columns to their postgres equivalents
            alter_column_sql = []
            for column_name, column_type in special_columns.items():
                delta_type = SPECIAL_TYPES_MAPPING[column_type]["postgres"].format(column_name=column_name)
                alter_column_sql.append(
                    f"ALTER TABLE {temp_destination_table}" f" ALTER COLUMN {column_name} TYPE {delta_type}"
                )
            if alter_column_sql:
                with db.connection.cursor() as cursor:
                    cursor.execute(";".join(alter_column_sql))

            if source_table:
                # Load indexes if applicable
                with db.connection.cursor() as cursor:
                    # Copy over the indexes, preserving the names (mostly, includes "_temp")
                    # Note: We could of included indexes above (`INCLUDING INDEXES`) but that renames them,
                    #       which would run into issues with migrations that have specific names.
                    #       Additionally, we want to run this after we loaded in the data for performance.
                    copy_index_sql = make_copy_indexes(cursor, source_table, temp_destination_table)
                    if copy_index_sql:
                        cursor.execute("; ".join(copy_index_sql))

        if spark_created_by_command:
            spark.stop()
