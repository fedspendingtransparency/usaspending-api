import boto3
import io
import logging
import math
import time

from boto3.s3.transfer import TransferConfig, S3Transfer
from botocore.exceptions import ClientError
from django.conf import settings
from pathlib import Path
from typing import List
from botocore.client import BaseClient

from usaspending_api.config import CONFIG

logger = logging.getLogger("script")


def _get_boto3_s3_client(region_name=CONFIG.AWS_REGION) -> BaseClient:
    """Returns the correct boto3 client based on the
    environment.

    Returns:
        BaseClient: Boto3 client implementatoin
    """
    if not CONFIG.USE_AWS:
        boto3_session = boto3.session.Session(
            region_name=region_name,
            aws_access_key_id=CONFIG.AWS_ACCESS_KEY.get_secret_value(),
            aws_secret_access_key=CONFIG.AWS_SECRET_KEY.get_secret_value(),
        )
        s3_client = boto3_session.client(
            service_name="s3",
            region_name=region_name,
            endpoint_url=f"http://{CONFIG.AWS_S3_ENDPOINT}",
        )
    else:
        s3_client = boto3.client(
            service_name="s3",
            region_name=region_name,
            endpoint_url=f"https://{CONFIG.AWS_S3_ENDPOINT}",
        )
    return s3_client


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


def upload_download_file_to_s3(file_path, sub_dir=None):
    bucket = settings.BULK_DOWNLOAD_S3_BUCKET_NAME
    region = settings.USASPENDING_AWS_REGION
    keyname = file_path.name
    multipart_upload(bucket, region, str(file_path), keyname, sub_dir)


def multipart_upload(bucketname, regionname, source_path, keyname, sub_dir=None):
    s3client = boto3.client("s3", region_name=regionname)
    source_size = Path(source_path).stat().st_size
    # Sets the chunksize at minimum ~5MB to sqrt(5MB) * sqrt(source size)
    bytes_per_chunk = max(int(math.sqrt(5242880) * math.sqrt(source_size)), 5242880)
    config = TransferConfig(multipart_chunksize=bytes_per_chunk)
    transfer = S3Transfer(s3client, config)
    file_name = Path(keyname).name
    if sub_dir is not None:
        file_name = f"{sub_dir}/{file_name}"
    transfer.upload_file(source_path, bucketname, file_name, extra_args={"ACL": "bucket-owner-full-control"})


def download_s3_object(
    bucket_name: str,
    key: str,
    file_path: str,
    s3_client: BaseClient = None,
    retry_count: int = 3,
    retry_cooldown: int = 30,
    region_name: str = settings.USASPENDING_AWS_REGION,
):
    """Download an S3 object to a file.
    Args:
        bucket_name: The name of the bucket where the key is located.
        key: The name of the key to download from.
        file_path: The path to the file to download to.
        max_retries: The number of times to retry the download.
        retry_delay: The amount of time in seconds to wait after a failure before retrying.
        region_name: AWS region
    """
    if not s3_client:
        s3_client = _get_boto3_s3_client(region_name)
    for attempt in range(retry_count + 1):
        try:
            s3_client.download_file(bucket_name, key, file_path)
            return
        except ClientError as e:
            logger.info(
                f"Attempt {attempt + 1} of {retry_count + 1} failed to download {key} from bucket {bucket_name}. Error: {e}"
            )
            if attempt < retry_count:
                time.sleep(retry_cooldown)
            else:
                logger.error(f"Failed to download {key} from bucket {bucket_name} after {retry_count + 1} attempts.")
                raise e


def delete_s3_object(bucket_name: str, key: str, region_name: str = settings.USASPENDING_AWS_REGION):
    """Delete an S3 object
    Args:
        bucket_name: The name of the bucket where the key is located.
        key: The name of the key to delete
    """
    s3 = _get_boto3_s3_client(region_name)
    s3.delete_object(Bucket=bucket_name, Key=key)
