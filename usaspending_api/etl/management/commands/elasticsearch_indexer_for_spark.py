import logging


from usaspending_api.common.helpers.spark_helpers import get_active_spark_session, configure_spark_session
from usaspending_api.etl.management.commands.elasticsearch_indexer import AbstractElasticsearchIndexer
from usaspending_api.etl.elasticsearch_loader_helpers.controller import AbstractElasticsearchIndexerController
from usaspending_api.etl.elasticsearch_loader_helpers.controller_for_spark import (
    DeltaLakeElasticsearchIndexerController,
)

logger = logging.getLogger("script")


class Command(AbstractElasticsearchIndexer):
    """Parallelized Spark-based ETL script for indexing Delta Lake data into Elasticsearch

    NOTE: Careful choosing how many executors to run, as the ES cluster can be easily overwhelmed.
    32 executors processing 10,000 record partitions seems to work for a 5-node ES cluster. 48 also,
    but might be pushing it.Increasing the ES cluster node count did not increase indexing speed, only doubling data
    node instance sizes did.
    """

    def create_controller(self, config: dict) -> AbstractElasticsearchIndexerController:
        extra_conf = {
            # Config for Delta Lake tables and SQL. Need these to keep Dela table metadata in the metastore
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # See comment below about old date and time values cannot be parsed without these
            "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
            "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
            "spark.sql.jsonGenerator.ignoreNullFields": "false",  # keep nulls in our json
        }
        spark_created_by_command = False
        spark = get_active_spark_session()
        if not spark:
            spark_created_by_command = True
            spark = configure_spark_session(**extra_conf, spark_context=spark)

        return DeltaLakeElasticsearchIndexerController(config, spark, spark_created_by_command)
