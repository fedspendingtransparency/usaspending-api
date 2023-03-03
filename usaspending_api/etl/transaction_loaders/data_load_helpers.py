from datetime import datetime
import os
import re
import boto3
import csv
import logging
import dateutil

from django.conf import settings

logger = logging.getLogger("script")


def capitalize_if_string(val):
    try:
        return val.upper()
    except AttributeError:
        return val


# 10/31/2019: According to PO direction, this functionality is NOT desired, and should be phased out as soon as it's safe
def false_if_null(val):
    if val is None:
        return False
    return val


def truncate_timestamp(val):
    if isinstance(val, datetime):
        return val.date()
    elif isinstance(val, str):
        return dateutil.parser.parse(val).date()
    elif val is None:
        return None
    else:
        raise ValueError("{} is not parsable as a date!".format(val.type))


def format_value_for_sql(val, cur):
    return str(cur.mogrify("%s", (val,)), "utf-8")


def format_bulk_insert_list_column_sql(cursor, load_objects, type):
    """creates formatted sql text to put into a bulk insert statement"""
    keys = load_objects[0][type].keys()

    columns = ['"{}"'.format(key) for key in load_objects[0][type].keys()]
    values = [[format_value_for_sql(load_object[type][key], cursor) for key in keys] for load_object in load_objects]

    col_string = "({})".format(",".join(columns))
    val_string = ",".join(["({})".format(",".join(map(str, value))) for value in values])

    return col_string, val_string


def format_insert_or_update_column_sql(cursor, load_object, type):
    """creates formatted sql text to put into a single row insert or update statement"""
    columns = []
    values = []
    update_pairs = []
    for key in load_object[type].keys():
        columns.append('"{}"'.format(key))
        val = format_value_for_sql(load_object[type][key], cursor)
        values.append(val)
        if key not in ["create_date", "created_at"]:
            update_pairs.append(" {}={}".format(key, val))

    col_string = "({})".format(",".join(map(str, columns)))
    val_string = "({})".format(",".join(map(str, values)))
    pairs_string = ",".join(update_pairs)

    return col_string, val_string, pairs_string


def get_deleted_fpds_data_from_s3(date):
    ids_to_delete = []
    regex_str = ".*_delete_records_(IDV|award).*"

    if not date:
        return []

    if settings.IS_LOCAL:
        for file in os.listdir(settings.CSV_LOCAL_PATH):
            if re.search(regex_str, file) and datetime.strptime(file[: file.find("_")], "%m-%d-%Y").date() >= date:
                with open(settings.CSV_LOCAL_PATH + file, "r") as current_file:
                    # open file, split string to array, skip the header
                    reader = csv.reader(current_file.read().splitlines())
                    next(reader)
                    unique_key_list = [rows[0] for rows in reader]

                    ids_to_delete += unique_key_list
    else:
        # Connect to AWS
        aws_region = settings.USASPENDING_AWS_REGION
        DELETED_TRANSACTION_JOURNAL_FILES = settings.DELETED_TRANSACTION_JOURNAL_FILES

        if not (aws_region and DELETED_TRANSACTION_JOURNAL_FILES):
            raise Exception(
                "Missing required environment variables: USASPENDING_AWS_REGION, DELETED_TRANSACTION_JOURNAL_FILES"
            )

        s3client = boto3.client("s3", region_name=aws_region)
        s3resource = boto3.resource("s3", region_name=aws_region)
        s3_bucket = s3resource.Bucket(DELETED_TRANSACTION_JOURNAL_FILES)

        # make an array of all the keys in the bucket
        file_list = [item.key for item in s3_bucket.objects.all()]

        # Only use files that match the date we're currently checking
        for item in file_list:
            # if the date on the file is the same day as we're checking
            if (
                re.search(regex_str, item)
                and "/" not in item
                and datetime.strptime(item[: item.find("_")], "%m-%d-%Y").date() >= date
            ):
                s3_item = s3client.get_object(Bucket=DELETED_TRANSACTION_JOURNAL_FILES, Key=item)
                reader = csv.reader(s3_item["Body"].read().decode("utf-8").splitlines())

                # skip the header, the reader doesn't ignore it for some reason
                next(reader)
                # make an array of all the detached_award_procurement_ids
                unique_key_list = [rows[0] for rows in reader]

                ids_to_delete += unique_key_list

    logger.info("Number of records to delete: %s" % str(len(ids_to_delete)))
    return ids_to_delete
