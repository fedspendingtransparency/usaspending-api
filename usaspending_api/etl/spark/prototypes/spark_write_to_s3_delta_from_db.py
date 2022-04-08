"""Read a DataFrame from a Postgres DB, then it to an S3 Bucket in Delta Lake table format

    Running Locally:
        (1) Review config values in usaspending_api/app_config/local.py and override any as needed in
            a .env file
        (2) Deploy minio in docker container (see README.md)
        (3) Make sure the bucket given by the value of config setting APP_CONFIG.AWS_S3_BUCKET exists
            - e.g. create via UI at http://localhost:9000
            - or use MinIO client CLI: mc mb local/data
        (4) Run with these dependent packages:
            (NOTEs:
             - hadoop-aws is needed for the S3AFileSystem.java, used to write data to S3,
             - and should use the same hadoop version in your local setup
             - and aws-java-sdk is used by hadoop-aws
             - For Spark 3.1.x, EMR uses hadoop-aws:3.2.1 and aws-java-sdk:1.12.31
            )
            (4.1) Run with spark-submit container as "Driver" (preferred):
            export JDBC_URL='...your DB JDBC URL in JDBC format!...'
            # example: export JDBC_URL='jdbc:postgresql://usaspending-db:5432/data_store_api?user=usaspending&password=usaspender'
            make docker-compose-up args='-d usaspending-db'
            make docker-compose-run args='--rm -e MINIO_HOST=minio -e JDBC_URL -e TABLE_NAME=awards -e APP_NAME='Write DB DF to S3 Delta Table' spark-submit \
                --packages org.postgresql:postgresql:42.2.23,io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.1,com.amazonaws:aws-java-sdk:1.12.31, \
                /project/usaspending_api/etl/spark/prototypes/spark_write_to_s3_delta_from_db.py'
            (4.2) Run with desktop dev env as "Driver":
            export JDBC_URL='...your DB JDBC URL in JDBC format!...'
            # example: export JDBC_URL='jdbc:postgresql://localhost:5432/data_store_api?user=usaspending&password=usaspender'
            make docker-compose-up args='-d usaspending-db'
            SPARK_LOCAL_IP=127.0.0.1 spark-submit \
                --packages org.postgresql:postgresql:42.2.23,io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.1,com.amazonaws:aws-java-sdk:1.12.31, \
                usaspending_api/etl/spark/spark_write_to_s3_delta_from_db.py
        (5) Browse data in the UI at http://localhost:9000
"""
import os

from usaspending_api.app_config import APP_CONFIG
from usaspending_api.common.helpers.spark_helpers import configure_spark_session
from usaspending_api.common.helpers.spark_helpers import get_jvm_logger
from pyspark.sql import SparkSession

PARTITION_ROWS = 10000
JDBC_CONN_PROPS = {"driver": "org.postgresql.Driver", "fetchsize": str(PARTITION_ROWS)}
JDBC_URL_KEY = "JDBC_URL"
TABLE_NAME_KEY = "TABLE_NAME"


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

    jdbc_url = os.environ.get(JDBC_URL_KEY)
    if not jdbc_url:
        raise RuntimeError(f"Looking for JDBC URL passed to env var '{JDBC_URL_KEY}', but not set.")
    if not jdbc_url.startswith("jdbc:postgresql://"):
        raise ValueError("JDBC URL given is not in postgres JDBC URL format (e.g. jdbc:postgresql://...")

    table_name = os.environ.get(TABLE_NAME_KEY)
    if not table_name:
        raise RuntimeError(f"Looking for table name passed to env var '{TABLE_NAME_KEY}', but not set.")

    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=JDBC_CONN_PROPS)

    db_name = "default"
    # NOTE! NOTE! NOTE! MinIO locally does not support a TRAILING SLASH after object (folder) name
    path = f"s3a://{APP_CONFIG.AWS_S3_BUCKET}/{APP_CONFIG.AWS_S3_OUTPUT_PATH}/{table_name}"

    log = get_jvm_logger(spark, __name__)
    log.info(f"Loading {df.count()} rows from DB to Delta table named {db_name}.{table_name} at path {path}")

    # Create table in the metastore using DataFrame's schema and write data to the table
    df.write.saveAsTable(
        format="delta",
        name=f"{db_name}.{table_name}",
        mode="overwrite",
        # overwriteSchema=True,
        # partitionBy="fiscal_year",  # optional partition by field, needs overwriteSchema
        path=path,
    )

    spark.stop()


if __name__ == "__main__":
    main()
