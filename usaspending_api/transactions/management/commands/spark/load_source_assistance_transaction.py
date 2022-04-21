import os
import sys

from usaspending_api.broker.helpers.last_load_date import get_last_load_date
from usaspending_api.common.helpers.sql_helpers import execute_sql_simple
from usaspending_api.config import CONFIG
from usaspending_api.common.etl.spark import (
    extract_db_data_frame,
    get_partition_bounds_sql,
    load_delta_table,
    merge_delta_table,
)
from usaspending_api.common.helpers.spark_helpers import configure_spark_session, get_jdbc_url, get_jvm_logger
from pyspark.sql import SparkSession

JDBC_URL_KEY = "JDBC_URL"

PARTITION_ROWS = 10000 * 16
# Abort processing the data if it would yield more than this many partitions to process as individual tasks
MAX_PARTITIONS = 100000
JDBC_CONN_PROPS = {"driver": "org.postgresql.Driver", "fetchsize": str(PARTITION_ROWS)}


def main():
    extra_conf = {
        # Config for Delta Lake tables and SQL. Need these to keep Dela table metadata in the metastore
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # See comment below about old date and time values cannot parsed without these
        "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
        "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
    }
    spark = configure_spark_session(**extra_conf)  # type: SparkSession

    # Setup Logger
    logger = get_jvm_logger(spark)
    logger.info("PySpark Job started!")
    logger.info(
        f"""
    @       Python Version: {sys.version}
    @       Spark Version: {spark.version}
    @       Hadoop Version: {spark.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()}
    """
    )

    spark.sql("use bronze")

    # Resolve Parameters
    SOURCE_TABLE = os.environ.get("SOURCE_TABLE", "published_award_financial_assistance")
    DESTINATION_TABLE = os.environ.get("DESTINATION_TABLE", "source_assistance_transaction")

    EXTERNAL_LOAD_DATE_KEY = os.environ.get("EXTERNAL_LOAD_DATE_KEY", "source_assistance_transaction")

    PARTITION_COLUMN = os.environ.get("PARTITION_COLUMN", "published_award_financial_assistance_id")
    MERGE_COLUMN = os.environ.get("MERGE_COLUMN", "published_award_financial_assistance_id")
    LAST_UPDATE_COLUMN = os.environ.get("LAST_UPDATE_COLUMN", "updated_at")

    FULL_RELOAD = os.environ.get("FULL_RELOAD", False)

    jdbc_url = os.environ.get(JDBC_URL_KEY)

    if not jdbc_url:
        raise RuntimeError(f"Looking for JDBC URL passed to env var '{JDBC_URL_KEY}', but not set.")
    if not jdbc_url.startswith("jdbc:postgresql://"):
        raise ValueError("JDBC URL given is not in postgres JDBC URL format (e.g. jdbc:postgresql://...")

    # Create temporary view on top of table that includes date predicate
    source_entity = SOURCE_TABLE
    if not FULL_RELOAD:
        last_load_date = get_last_load_date(EXTERNAL_LOAD_DATE_KEY)
        source_entity = f"{SOURCE_TABLE}_LATEST"
        execute_sql_simple(
            f"CREATE OR REPLACE VIEW {source_entity} AS SELECT * FROM {SOURCE_TABLE} WHERE {LAST_UPDATE_COLUMN} > {last_load_date}"
        )

    # Read from table or view
    source_assistance_transaction_df = extract_db_data_frame(
        spark,
        JDBC_CONN_PROPS,
        get_jdbc_url(),
        PARTITION_ROWS,
        get_partition_bounds_sql(
            # TODO Correct this to point to source table info
            SOURCE_TABLE,
            PARTITION_COLUMN,
            PARTITION_COLUMN,
            is_partitioning_col_unique=False,
        ),
        source_entity,
        PARTITION_COLUMN,
    )

    # Write to S3
    if not FULL_RELOAD:
        load_delta_table(spark, source_assistance_transaction_df, f"{DESTINATION_TABLE}", True)
        execute_sql_simple(f"DROP VIEW {source_entity}")
    else:
        merge_delta_table(
            spark,
            source_assistance_transaction_df,
            f"{DESTINATION_TABLE}",
            MERGE_COLUMN,
        )

    spark.stop()


if __name__ == "__main__":
    main()
