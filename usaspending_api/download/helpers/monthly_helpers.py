import boto3
import logging
import math
import os

from django.conf import settings
from usaspending_api.references.models import ToptierAgency

logger = logging.getLogger("console")


def multipart_upload(bucketname, regionname, source_path, keyname):
    s3client = boto3.client("s3", region_name=regionname)
    source_size = os.stat(source_path).st_size
    # Sets the chunksize at minimum ~5MB to sqrt(5MB) * sqrt(source size)
    bytes_per_chunk = max(int(math.sqrt(5242880) * math.sqrt(source_size)), 5242880)
    config = boto3.s3.transfer.TransferConfig(multipart_chunksize=bytes_per_chunk)
    transfer = boto3.s3.transfer.S3Transfer(s3client, config)
    transfer.upload_file(source_path, bucketname, os.path.basename(keyname))


def write_to_download_log(message, download_job=None, is_debug=False, is_error=False, other_params={}):
    """Handles logging for the downloader instance"""
    if settings.IS_LOCAL:
        log_dict = message
    else:
        log_dict = {"message": message, "message_type": "USAspendingDownloader"}

        if download_job:
            log_dict["download_job_id"] = download_job.download_job_id
            log_dict["file_name"] = download_job.file_name
            log_dict["json_request"] = download_job.json_request
            if download_job.error_message:
                log_dict["error_message"] = download_job.error_message

        for param in other_params:
            if param not in log_dict:
                log_dict[param] = other_params[param]

    if is_error:
        logger.exception(log_dict)
    elif is_debug:
        logger.debug(log_dict)
    else:
        logger.info(log_dict)


def pull_modified_agencies_cgacs():
    return ToptierAgency.objects.filter(agency__user_selectable=True).values_list("toptier_code", flat=True)
