import boto3  # noqa
import csv
import logging
import re

from collections import defaultdict
from datetime import datetime
from django.conf import settings
from typing import Optional, List

from usaspending_api.common.helpers.date_helper import datetime_is_ge, datetime_is_lt
from usaspending_api.common.helpers.s3_helpers import access_s3_object, retrieve_s3_bucket_object_list
from usaspending_api.common.helpers.timing_helpers import ScriptTimer as Timer


logger = logging.getLogger("script")


def retrieve_deleted_fabs_transactions(start_datetime: datetime, end_datetime: Optional[datetime] = None) -> dict:
    FABS_regex = r".*_FABSdeletions_(?P<epoch>\d+)\.csv"
    return retrieve_deleted_transactions(FABS_regex, start_datetime, end_datetime)


def retrieve_deleted_fpds_transactions(start_datetime: datetime, end_datetime: Optional[datetime] = None) -> dict:
    FPDS_regex = r".*_delete_records_(IDV|award)_(?P<epoch>\d+)\.csv"
    return retrieve_deleted_transactions(FPDS_regex, start_datetime, end_datetime)


def retrieve_deleted_transactions(
    regex: str, start_datetime: datetime, end_datetime: Optional[datetime] = None
) -> dict:
    with Timer("Obtaining S3 Object list"):
        objects = retrieve_s3_bucket_object_list(settings.DELETED_TRANSACTION_JOURNAL_FILES)
        logger.info(f"{len(objects):,} files found in bucket '{settings.DELETED_TRANSACTION_JOURNAL_FILES}'.")
        objects = [o for o in objects if re.fullmatch(regex, o.key) is not None]
        logger.info(f"{len(objects):,} files match file pattern '{regex}'.")
        objects = limit_objects_to_date_range(objects, regex, start_datetime, end_datetime)
        logger.info(
            f"{len(objects):,} files found in date range {start_datetime} through {end_datetime or 'the end of time'}."
        )

    deleted_records = defaultdict(list)
    for obj in objects:
        object_data = access_s3_object(settings.DELETED_TRANSACTION_JOURNAL_FILES, obj)
        reader = csv.reader(object_data.read().decode("utf-8").splitlines())
        next(reader)  # skip the header

        transaction_id_list = [rows[0] for rows in reader]
        logger.info(f"{len(transaction_id_list):,} delete ids found in {obj.key}")

        if transaction_id_list:
            file_date = obj.key[: obj.key.find("_")]
            deleted_records[file_date].extend(transaction_id_list)

    return deleted_records


def limit_objects_to_date_range(
    objects: List["boto3.resources.factory.s3.ObjectSummary"],
    regex_pattern: str,
    start_datetime: datetime,
    end_datetime: Optional[datetime] = None,
) -> List["boto3.resources.factory.s3.ObjectSummary"]:
    results = []
    for obj in objects or []:

        match = re.fullmatch(regex_pattern, obj.key)
        file_datetime = datetime.utcfromtimestamp(int(match["epoch"]))

        # Only keep S3 objects that fall between the provided dates
        if datetime_is_lt(file_datetime, start_datetime):
            continue
        if end_datetime and datetime_is_ge(file_datetime, end_datetime):
            continue

        results.append(obj)

    return results
