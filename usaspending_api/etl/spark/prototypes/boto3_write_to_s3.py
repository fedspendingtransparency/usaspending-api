"""Write a CSV file to an S3 Bucket using Boto3 lib

    Running Locally:
        (1) Review config values in usaspending_api/app_config/local.py and override any as needed in
            a .env file
        (2) Deploy minio in docker container (see README.md)
        (3) Make sure the bucket given by the value of config setting APP_CONFIG.AWS_S3_BUCKET exists
            - e.g. create via UI at http://localhost:9000
            - or use MinIO client CLI: mc mb local/data
        (4) Run:
            python3 usaspending_api/etl/spark/prototypes/boto3_write_to_s3.py
"""
import uuid
import random
import boto3
import csv

from botocore.exceptions import ClientError
from io import StringIO, BytesIO
from usaspending_api.app_config import APP_CONFIG


def main():
    data = [
        {"first_col": "row 1", "id": str(uuid.uuid4()), "color": "blue", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 2", "id": str(uuid.uuid4()), "color": "green", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 3", "id": str(uuid.uuid4()), "color": "pink", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 4", "id": str(uuid.uuid4()), "color": "yellow", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 5", "id": str(uuid.uuid4()), "color": "red", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 6", "id": str(uuid.uuid4()), "color": "orange", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 7", "id": str(uuid.uuid4()), "color": "magenta", "numeric_val": random.randint(-100, 100)},
    ]

    filelike_string_buffer = StringIO()
    writer = csv.DictWriter(filelike_string_buffer, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    filelike_string_buffer.seek(0)
    file_bytes = BytesIO(filelike_string_buffer.read().encode())  # note, reads all bytes in mem, no streaming

    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://{APP_CONFIG.AWS_S3_ENDPOINT}",
        aws_access_key_id=APP_CONFIG.AWS_ACCESS_KEY.get_secret_value(),
        aws_secret_access_key=APP_CONFIG.AWS_SECRET_KEY.get_secret_value(),
    )

    try:
        response = s3_client.upload_fileobj(
            file_bytes,
            APP_CONFIG.AWS_S3_BUCKET,
            f"{APP_CONFIG.AWS_S3_OUTPUT_PATH}/boto3_write_to_s3/boto3_write_to_s3.csv",
        )
    except ClientError as e:
        print(f"ERROR writing file with boto3\n{e}")
        print(f"RESPONSE:\n{response}")
        raise


if __name__ == "__main__":
    main()
