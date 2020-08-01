import boto3
import io
import logging
import math

from django.conf import settings
from pathlib import Path
from typing import List


logger = logging.getLogger("script")


def retrieve_s3_bucket_object_list(bucket_name: str) -> List["boto3.resources.factory.s3.ObjectSummary"]:
    try:
        bucket = get_s3_bucket(bucket_name=bucket_name)
        bucket_objects = list(bucket.objects.all())
    except Exception as e:
        message = (
            f"Problem accessing S3 bucket '{bucket_name}' for deleted records.  Most likely the "
            f"AWS region or bucket name is configured incorrectly."
        )
        logger.exception(message)
        raise RuntimeError(message) from e
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


def upload_download_file_to_s3(file_path):
    bucket = settings.BULK_DOWNLOAD_S3_BUCKET_NAME
    region = settings.USASPENDING_AWS_REGION
    multipart_upload(bucket, region, str(file_path), file_path.name)


def multipart_upload(bucketname, regionname, source_path, keyname):
    s3client = boto3.client("s3", region_name=regionname)
    source_size = Path(source_path).stat().st_size
    # Sets the chunksize at minimum ~5MB to sqrt(5MB) * sqrt(source size)
    bytes_per_chunk = max(int(math.sqrt(5242880) * math.sqrt(source_size)), 5242880)
    config = boto3.s3.transfer.TransferConfig(multipart_chunksize=bytes_per_chunk)
    transfer = boto3.s3.transfer.S3Transfer(s3client, config)
    transfer.upload_file(source_path, bucketname, Path(keyname).name)
