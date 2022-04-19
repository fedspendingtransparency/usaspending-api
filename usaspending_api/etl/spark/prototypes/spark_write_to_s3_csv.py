"""Write a DataFrame to an S3 Bucket

    Running Locally:
        (1) Review config values in usaspending_api/app_config/local.py and override any as needed in
            a .env file
        (2) Deploy minio in docker container (see README.md)
        (3) Make sure the bucket given by the value of config setting CONFIG.AWS_S3_BUCKET exists
            - e.g. create via UI at http://localhost:9000
            - or use MinIO client CLI: mc mb local/data
        (4) Run with spark-submit container as "Driver" (preferred):
            make docker-compose-run args="--rm -e MINIO_HOST=minio -e APP_NAME='Write CSV to S3' spark-submit \
                --packages org.apache.hadoop:hadoop-aws:3.2.1,com.amazonaws:aws-java-sdk:1.12.31 \
                /project/usaspending_api/etl/spark/prototypes/spark_write_to_s3_csv.py"
            Run with desktop dev env as "Driver":
            SPARK_LOCAL_IP=127.0.0.1 spark-submit \
                --packages org.apache.hadoop:hadoop-aws:3.2.1,com.amazonaws:aws-java-sdk:1.12.31 \
                usaspending_api/etl/spark/prototypes/spark_write_to_s3_csv.py

"""
import uuid
import random

from usaspending_api.config import CONFIG
from usaspending_api.common.helpers.spark_helpers import configure_spark_session
from pyspark.sql import SparkSession, Row


def main():
    spark = configure_spark_session()  # type: SparkSession

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
    df.write.option("header", True).csv(
        f"s3a://{CONFIG.AWS_S3_BUCKET}/{CONFIG.AWS_S3_OUTPUT_PATH}/write_to_s3/"
    )

    spark.stop()


if __name__ == "__main__":
    main()
