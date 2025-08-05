import boto3
import logging
import time

from datetime import datetime, timezone
from django.conf import settings


logger = logging.getLogger("script")


def store_deleted_fabs(ids_to_delete):
    seconds = int(time.time())  # adds enough uniqueness to filename
    file_name = f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}_FABSdeletions_{seconds}.csv"
    rows_with_header = ["afa_generated_unique"] + list(ids_to_delete)

    if settings.IS_LOCAL:
        file_path = settings.CSV_LOCAL_PATH + file_name
        logger.info(f"storing deleted transaction IDs at: {file_path}")
        with open(file_path, "w") as f:
            for row in rows_with_header:
                f.write(row + "\n")
    else:
        logger.info("Uploading FABS delete data to S3 bucket")
        contents = bytes()
        for row in rows_with_header:
            contents += bytes(f"{row}\n".encode())

        s3client = boto3.client("s3", region_name=settings.USASPENDING_AWS_REGION)
        s3client.put_object(Bucket=settings.DELETED_TRANSACTION_JOURNAL_FILES, Key=file_name, Body=contents)
