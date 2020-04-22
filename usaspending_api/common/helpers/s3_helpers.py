import boto3
import io
import logging
import re

from typing import Optional, List

from usaspending_api import settings

logger = logging.getLogger("script")


def access_s3_object_list(
    bucket_name: str, regex_pattern: Optional[str] = None
) -> List["boto3.resources.factory.s3.ObjectSummary"]:
    """Find all S3 objects in provided bucket.

    If regex is passed, only keys which match the regex are returned
    """

    logger.info("Gathering all deleted transactions from S3")
    try:
        bucket = get_s3_bucket(bucket_name=bucket_name)
        bucket_objects = list(bucket.objects.all())
        logger.info(f"{len(bucket_objects):,} files found in bucket.")
    except Exception:
        logger.exception("Most likely the AWS region or bucket name are configured incorrectly")
        bucket_objects = None

    if regex_pattern and bucket_objects:
        bucket_objects = [obj for obj in bucket_objects if re.search(regex_pattern, obj.key)]
        logger.info(f"{len(bucket_objects):,} files matched file pattern.")

    return bucket_objects


def get_s3_bucket(
    bucket_name: str, region_name: str = settings.USASPENDING_AWS_REGION
) -> "boto3.resources.factory.s3.Instance":
    s3 = boto3.resource("s3", region_name=region_name)
    return s3.Bucket(bucket_name)


def access_s3_object(bucket_name: str, obj: "boto3.resources.factory.s3.ObjectSummary") -> io.BytesIO:
    """Return the Bytes of an S3 object"""
    bucket = get_s3_bucket(bucket_name=bucket_name)
    data = io.BytesIO()
    bucket.download_fileobj(obj.key, data)
    data.seek(0)  # Like rewinding a VCR cassette
    return data
