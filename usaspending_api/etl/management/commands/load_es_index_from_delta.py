import logging

from django.core.management.base import BaseCommand
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime

from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
    get_jvm_logger,
    get_es_config,
)
from usaspending_api.common.etl.spark import load_es_index, convert_decimal_cols_to_string

from usaspending_api.etl.management.commands.create_delta_table import TABLE_SPEC


class Command(BaseCommand):

    logger: logging.Logger

    # TODO: expand help docs
    help = """
    This command reads data from a Delta table and copies it into a corresponding Elasticsearch index using the 
    ``index_name`` parameter. It only supports a full reload of an index, and the target ``index_name`` must not 
    exist yet when this is run.
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
            help="An alternate delta table name to load, overriding the TABLE_SPEC --delta-table name",
        )
        # TODO: eventually may change to --load-type, so details can be looked up from the elasticsearch_indexer.py
        #  dict config. This should also use that config's value for routing key rather than the arg here.
        parser.add_argument(
            "--index-base-name",
            type=str,
            required=False,
            help="The base name of the target Elasticsearch Index to be created and loaded with the Delta Table data. "
                 "This base name will have --datetime-label (or this arg's default) added as a suffix to it to make "
                 "it unique upon (re)creations. If not provided, will default to the value of --delta-table.",
        )
        parser.add_argument(
            "--routing-field",
            type=str,
            required=False,
            help="The field in the JSON doc being index whose value will be the ID field for the document. Must be "
                 "unique. Defaults to _id (internal/generated ES doc ID) if not provided.",
            default="_id",
        )
        parser.add_argument(
            "--doc-id-field",
            type=str,
            required=False,
            help="The field in the JSON doc being index whose value will be used to determine which shard of the "
                 "index it is routed to. Uses default ES strategy if not provided.",
        )
        parser.add_argument(
            "--datetime-label",
            help="Date label in YYYY-MM-DD-HH-mm format to put on file names and ES indices. Defaults to current "
                 "date+time if not provided.",
            default=datetime.now().strftime("%Y-%m-%d-%H-%M"),
        )

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
        self.logger = get_jvm_logger(spark, __name__)

        # Resolve Parameters
        delta_table = options["delta_table"]
        index_base_name = options["index_base_name"] or delta_table
        index_name = f"{index_base_name}-{options['datetime_label']}"
        routing_field = options["routing_field"]
        doc_id_field = options["doc_id_field"]

        table_spec = TABLE_SPEC[delta_table]

        # Delta side
        delta_db_name = options["alt_delta_db"] or table_spec["destination_database"]
        delta_table_name = options["alt_delta_name"] or delta_table
        delta_table = f"{delta_db_name}.{delta_table_name}" if delta_db_name else delta_table_name

        summary_msg = f"Copying delta table {delta_table} to new ES index {index_name}."
        self.logger.info(summary_msg)

        # Checking if the index already exists
        # TODO: check for index name collision (already existing). Then probably error out and force it to be removed
        #  or a different name used.

        # Read from Delta
        df = spark.table(delta_table)  # type: DataFrame

        # ES does not intake DecimalType cols, due to risk of losing precision. Cast to StringType, and use fixed
        # type in index mapping or index mapping template to which it will be cast on write
        df_no_decimal = convert_decimal_cols_to_string(df)

        # Make sure that the column order defined in the Delta table schema matches
        # that of the Spark dataframe used to pull from the Postgres table. While not
        # always needed, this should help to prevent any future mismatch between the two.
        # TODO: decide if columns of delta table will be narrowed by a TEMP VIEW, or if we need to select them here
        # if column_names:
        #     df = df.select(column_names)

        # Write to Elasticsearch
        self.logger.info(f"LOAD (START): Loading data from Delta table {delta_table} to ES {index_name} index")

        try:
            load_es_index(
                spark=spark,
                source_df=df_no_decimal,
                base_config=get_es_config(),
                index_name=index_name,
                routing=routing_field,
                doc_id=doc_id_field,
            )

        except Exception as exc:
            self.logger.exception(
                f"Command failed unexpectedly; index where data was being staged should be deleted: {index_name}",
                exc,
            )
            raise Exception(exc)

        self.logger.info(
            f"LOAD (FINISH): Loaded data from Delta table {delta_table} to ES {index_name} index"
        )

        # We're done with spark at this point
        if spark_created_by_command:
            spark.stop()
