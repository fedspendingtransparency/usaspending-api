import boto3
import io
import logging
import math

from boto3.s3.transfer import TransferConfig, S3Transfer
from botocore.client import BaseClient

from django.conf import settings
from pathlib import Path
from typing import List, Union
from contextlib import closing

from usaspending_api.config import CONFIG


logger = logging.getLogger("script")


def get_boto3_s3_client() -> BaseClient:
    if not CONFIG.USE_AWS:
        boto3_session = boto3.session.Session(
            region_name=CONFIG.AWS_REGION,
            aws_access_key_id=CONFIG.AWS_ACCESS_KEY.get_secret_value(),
            aws_secret_access_key=CONFIG.AWS_SECRET_KEY.get_secret_value(),
        )
        s3_client = boto3_session.client(
            service_name="s3",
            region_name=CONFIG.AWS_REGION,
            endpoint_url=f"http://{CONFIG.AWS_S3_ENDPOINT}",
        )
    else:
        s3_client = boto3.client(
            service_name="s3",
            region_name=CONFIG.AWS_REGION,
            endpoint_url=f"https://{CONFIG.AWS_S3_ENDPOINT}",
        )
    return s3_client


def get_s3_object_bytes(s3_bucket_name: str, s3_obj_key: str, configured_logger=None) -> bytes:
    if not configured_logger:
        configured_logger = logger
    s3_client = get_boto3_s3_client()
    try:
        s3_obj = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_obj_key)
        # Getting Body gives a botocore.response.StreamingBody object back to allow "streaming" its contents
        s3_obj_body = s3_obj["Body"]
        with closing(s3_obj_body):  # make sure to close the stream when done
            obj_bytes = s3_obj_body.read()
            configured_logger.info(
                f"Finished reading s3 object bytes for '{s3_obj_key}' from bucket '{s3_bucket_name}'"
            )
            return obj_bytes
    except Exception as exc:
        configured_logger.error(f"ERROR reading object '{s3_obj_key}' from bucket '{s3_bucket_name}'")
        configured_logger.exception(exc)
        raise exc


def download_s3_object(
    s3_bucket_name: str, s3_obj_key: str, download_path: Union[str, Path], configured_logger=None
) -> None:
    try:
        with open(download_path, "wb") as csv_file:
            csv_file.write(
                get_s3_object_bytes(
                    s3_bucket_name=s3_bucket_name,
                    s3_obj_key=s3_obj_key,
                    configured_logger=configured_logger,
                )
            )

        configured_logger.info(
            f"Finished writing s3 object '{s3_obj_key}' from bucket '{s3_bucket_name}' to path '{str(download_path)}'"
        )
    except Exception as exc:
        configured_logger.error(f"ERROR downloading object '{s3_obj_key}' from bucket '{s3_bucket_name}'")
        configured_logger.exception(exc)
        raise exc


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
    config = TransferConfig(multipart_chunksize=bytes_per_chunk)
    transfer = S3Transfer(s3client, config)
    transfer.upload_file(source_path, bucketname, Path(keyname).name, extra_args={"ACL": "bucket-owner-full-control"})
