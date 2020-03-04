import csv
import logging

from collections import defaultdict
from datetime import datetime
from typing import Optional, List

from usaspending_api import settings
from usaspending_api.common.helpers.timing_helpers import ScriptTimer as Timer
from usaspending_api.common.helpers.s3_helpers import access_s3_object, access_s3_object_list


logger = logging.getLogger("script")


def retrieve_deleted_fabs_transactions(start_datetime: datetime, end_datetime: Optional[datetime] = None) -> dict:
    FABS_regex = ".*_FABSdeletions_.*"
    date_format = "%Y-%m-%d"
    return retrieve_deleted_transactions(FABS_regex, date_format, start_datetime, end_datetime)


def retrieve_deleted_fpds_transactions(start_datetime: datetime, end_datetime: Optional[datetime] = None) -> dict:
    FPDS_regex = ".*_delete_records_(IDV|award).*"
    date_format = "%m-%d-%Y"
    return retrieve_deleted_transactions(FPDS_regex, date_format, start_datetime, end_datetime)


def retrieve_deleted_transactions(
    regex: str, date_fmt: str, start_datetime: datetime, end_datetime: Optional[datetime] = None
) -> dict:
    with Timer("Obtaining S3 Object list"):
        objects = access_s3_object_list(settings.DELETED_TRANSACTION_JOURNAL_FILES, regex_pattern=regex)
        if objects is None:
            raise Exception("Problem accessing S3 for deleted records")
        objects = limit_objects_to_date_range(objects, date_fmt, start_datetime, end_datetime)

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
) -> List["boto3.resources.factory.s3.ObjectSummary"]:
    results = []
    for obj in objects or []:
        file_date = datetime.strptime(obj.key[: obj.key.find("_")], date_format)
        # Only keep S3 objects which fall between the provided dates
        if file_date < start_datetime:
            continue
        if end_datetime and file_date >= end_datetime:
            continue
        results.append(obj)

    return results
