"""Write a DataFrame to an S3 Bucket in Delta Lake table format

    Running Locally:
        (1) Review config values in usaspending_api/app_config/local.py and override any as needed in
            a .env file
        (2) Deploy minio in docker container (see README.md)
        (3) Make sure the bucket given by the value of config setting CONFIG.AWS_S3_BUCKET exists
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
            make docker-compose-run args="--rm -e MINIO_HOST=minio -e APP_NAME='Write CSV to S3' spark-submit \
                --packages org.postgresql:postgresql:42.2.23,io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.1,com.amazonaws:aws-java-sdk:1.12.31, \
                /project/usaspending_api/etl/spark/prototypes/spark_write_to_s3_delta.py"
            (4.2) Run with desktop dev env as "Driver":
            SPARK_LOCAL_IP=127.0.0.1 spark-submit \
                --packages org.postgresql:postgresql:42.2.23,io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.1,com.amazonaws:aws-java-sdk:1.12.31, \
                usaspending_api/etl/spark/prototypes/spark_write_to_s3_delta.py
        (5) Browse data in the UI at http://localhost:9000
"""
import uuid
import random

from usaspending_api.config import CONFIG
from usaspending_api.common.helpers.spark_helpers import configure_spark_session
from pyspark.sql import SparkSession, Row


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

    data = [
        {"first_col": "row 1", "id": str(uuid.uuid4()), "color": "blue", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 2", "id": str(uuid.uuid4()), "color": "green", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 3", "id": str(uuid.uuid4()), "color": "pink", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 4", "id": str(uuid.uuid4()), "color": "yellow", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 5", "id": str(uuid.uuid4()), "color": "red", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 6", "id": str(uuid.uuid4()), "color": "orange", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 7", "id": str(uuid.uuid4()), "color": "magenta", "numeric_val": random.randint(-100, 100)},
    ]

    df = spark.createDataFrame([Row(**data_row) for data_row in data])

    db_name = "default"
    table_name = "write_to_s3_delta"
    # NOTE! NOTE! NOTE! MinIO locally does not support a TRAILING SLASH after object (folder) name
    path = f"s3a://{CONFIG.AWS_S3_BUCKET}/{CONFIG.AWS_S3_OUTPUT_PATH}/{table_name}"

    # Create table in the metastore using DataFrame's schema and write data to the table
    df.write.saveAsTable(
        format="delta",
        name=f"{db_name}.{table_name}",
        mode="overwrite",
        partitionBy="color",
        path=path,
    )

    # Alternatively, create the delta table files directly at the given S3 path
    # df.write.format("delta").mode("overwrite").partitionBy("color").save(path)

    spark.stop()


if __name__ == "__main__":
    main()
