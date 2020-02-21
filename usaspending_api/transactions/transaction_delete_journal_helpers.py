import boto3
import csv
import io
import logging
import re

from collections import defaultdict
from datetime import datetime
from typing import Optional, List

from usaspending_api import settings
from usaspending_api.common.helpers.timing_helpers import Timer


logger = logging.getLogger("script")


def retrieve_deleted_fabs_transactions(start_datetime: datetime, end_datetime: Optional[datetime] = None) -> dict:
    FABS_regex = ".*_FABSdeletions_.*"
    date_format = "%Y-%m-%d"
    with Timer("Obtaining S3 Object list"):
        objects = access_s3_object_list(settings.DELETED_TRANSACTION_JOURNAL_FILES, regex_pattern=FABS_regex)
        objects = limit_objects_to_date_range(objects, date_format, start_datetime, end_datetime)

    deleted_records = defaultdict(list)
    for obj in objects:
        object_data = access_s3_object(settings.DELETED_TRANSACTION_JOURNAL_FILES, obj)
        reader = csv.reader(object_data.read().decode("utf-8").splitlines())
        next(reader)  # skip the header

        transaction_id_list = [rows[0] for rows in reader]

        if transaction_id_list:
            file_date = obj.key[: obj.key.find("_")]
            deleted_records[file_date].extend(transaction_id_list)

    return deleted_records


def retrieve_deleted_fpds_transactions(start_datetime: datetime, end_datetime: Optional[datetime] = None) -> dict:
    FPDS_regex = ".*_delete_records_(IDV|award).*"
    date_format = "%m-%d-%Y"
    with Timer("Obtaining S3 Object list"):
        objects = access_s3_object_list(settings.DELETED_TRANSACTION_JOURNAL_FILES, regex_pattern=FPDS_regex)
        objects = limit_objects_to_date_range(objects, date_format, start_datetime, end_datetime)

    deleted_records = defaultdict(list)
    for obj in objects:
        object_data = access_s3_object(settings.DELETED_TRANSACTION_JOURNAL_FILES, obj)
        reader = csv.reader(object_data.read().decode("utf-8").splitlines())
        next(reader)  # skip the header

        transaction_id_list = [rows[0] for rows in reader]

        if transaction_id_list:
            file_date = obj.key[: obj.key.find("_")]
            deleted_records[file_date].extend(transaction_id_list)

    return deleted_records


def limit_objects_to_date_range(
    objects: List["boto3.resources.factory.s3.ObjectSummary"],
    date_format: str,
    start_datetime: datetime,
    end_datetime: Optional[datetime] = None,
):
    results = []
    for obj in (objects or []):
        file_date = datetime.strptime(obj.key[: obj.key.find("_")], date_format)
        # Only keep S3 objects which fall between the provided dates
        if file_date < start_datetime:
            continue
        if end_datetime and file_date >= end_datetime:
            continue
        results.append(obj)

    return results


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
    except Exception:
        msg = (
            "Verify settings.USASPENDING_AWS_REGION and settings.DELETED_TRANSACTION_JOURNAL_FILES are correct "
            "or env variables: USASPENDING_AWS_REGION and DELETED_TRANSACTION_JOURNAL_FILES are set"
        )
        logger.exception(msg)
        bucket_objects = None

    if regex_pattern and bucket_objects:
        bucket_objects = [obj for obj in bucket_objects if re.search(regex_pattern, obj.key)]

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
