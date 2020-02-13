import boto3
import logging
import time

from datetime import datetime, timezone
from django.conf import settings


logger = logging.getLogger("console")


def store_deleted_fabs(ids_to_delete):
    seconds = int(time.time())  # adds enough uniqueness to filename
    file_name = f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}_FABSdeletions_{seconds}.csv"
    file_with_headers = ["afa_generated_unique"] + list(ids_to_delete)

    if settings.IS_LOCAL:
        file_path = settings.CSV_LOCAL_PATH + file_name
        logger.info(f"storing deleted transaction IDs at: {file_path}")
        with open(file_path, "w") as writer:
            for row in file_with_headers:
                writer.write(row + "\n")
    else:
        logger.info("Uploading FABS delete data to S3 bucket")
        aws_region = settings.USASPENDING_AWS_REGION
        DELETED_TRANSACTION_JOURNAL_S3_BUCKET = settings.DELETED_TRANSACTION_JOURNAL_S3_BUCKET
        s3client = boto3.client("s3", region_name=aws_region)
        contents = bytes()
        for row in file_with_headers:
            contents += bytes("{}\n".format(row).encode())
        s3client.put_object(Bucket=DELETED_TRANSACTION_JOURNAL_S3_BUCKET, Key=file_name, Body=contents)
