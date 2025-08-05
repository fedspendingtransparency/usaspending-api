import re
import boto3

from django.conf import settings
from typing import Optional

from usaspending_api.config import CONFIG


def get_last_modified_download_file(download_prefix: str, bucket_name: str) -> Optional[str]:
    """Return the last modified bulk download file from the bulk download bucket
    based on the provided file prefix.
    Args:
        bucket_name: The AWS bucket name in which to search for the file
        download_prefix: The prefix to filter files on in the bulk download bucket
    Returns:
        The file name of the last modified file.
    """
    download_regex = r"{}.*\.zip".format(download_prefix)

    # Retrieve and filter the files we need
    if settings.IS_LOCAL:
        boto3_session = boto3.session.Session(
            region_name=CONFIG.AWS_REGION,
            aws_access_key_id=CONFIG.AWS_ACCESS_KEY.get_secret_value(),
            aws_secret_access_key=CONFIG.AWS_SECRET_KEY.get_secret_value(),
        )
        s3_resource = boto3_session.resource(
            service_name="s3", region_name=CONFIG.AWS_REGION, endpoint_url=f"http://{CONFIG.AWS_S3_ENDPOINT}"
        )
        s3_bucket = s3_resource.Bucket(bucket_name)

    else:
        s3_bucket = boto3.resource("s3", region_name=settings.USASPENDING_AWS_REGION).Bucket(bucket_name)

    download_names = []
    for key in s3_bucket.objects.filter(Prefix=download_prefix):
        if re.match(download_regex, key.key):
            download_names.append(key)

    return get_last_modified_file(download_names)


def get_last_modified_file(download_files) -> Optional[str]:
    """Return the last modified file from the list of download files.
    Args:
        download_files (List[s3.ObjectSummary]): The files to identify the last modified file from.
    Returns:
        The file name of the last modified file.
    """
    # Best effort to identify the latest file by assuming the file with the latest last modified date
    # is the latest file to have been generated
    sorted_files = [obj.key for obj in sorted(download_files, key=get_last_modified_int, reverse=True)]

    last_added_file = None
    if len(sorted_files) > 0:
        last_added_file = sorted_files[0]
    return last_added_file


def get_last_modified_int(file) -> int:
    """Returns a number that can be used to sort files by the last modified date.
    Args:
        file: The file to retrieve the last modified date from.
    Returns:
        int: The number that represents the files last modified date.
    """
    return int(file.last_modified.strftime("%s"))


def remove_file_prefix_if_exists(file_name: str, prefix: str):
    return file_name.replace(f"{prefix}", "") if file_name is not None else None
