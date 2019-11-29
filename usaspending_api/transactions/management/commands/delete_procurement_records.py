import boto3
import csv
import io
import logging
import re

from collections import defaultdict
from datetime import datetime
from django.conf import settings
from django.core.management.base import BaseCommand

from usaspending_api.transactions.agnostic_transaction_deletes import AgnosticDeletes
from usaspending_api.transactions.models.source_procurement_transaction import SourceProcurmentTransaction

logger = logging.getLogger("script")


class Command(AgnosticDeletes, BaseCommand):
    help = "Delete procurement transactions in an USAspending database"
    destination_table_name = SourceProcurmentTransaction().table_name
    shared_pk = "detached_award_procurement_id"

    def fetch_deleted_transactions(self, date):
        ids_to_delete = defaultdict(list)
        regex_str = ".*_delete_records_(IDV|award).*"

        if settings.IS_LOCAL:
            logger.info("Local mode does not handle deleted records")
            return None

        s3 = boto3.resource("s3", region_name=settings.USASPENDING_AWS_REGION)
        s3_bucket = s3.Bucket(settings.FPDS_BUCKET_NAME)

        # Only use files that match the date we're currently checking
        for obj in s3_bucket.objects.all():
            key_name = obj.key
            if "/" in key_name or not re.search(regex_str, key_name):
                continue

            file_date = datetime.strptime(key_name[: key_name.find("_")], "%m-%d-%Y").date()

            if file_date < date:
                continue

            logger.info("========= {}".format(key_name))
            data = io.BytesIO()
            s3_bucket.download_fileobj(key_name, data)
            data.seek(0)
            reader = csv.reader(data.read().decode("utf-8").splitlines())
            next(reader)  # skip the header

            full_key_list = [rows[0] for rows in reader]
            numeric_keys = list([int(rows) for rows in full_key_list if rows.isnumeric()])
            odd_ids = set(full_key_list).symmetric_difference([str(id) for id in numeric_keys])

            if odd_ids:
                logger.info(f"Unexpected non-numeric IDs in file: {list(odd_ids)}")

            if numeric_keys:
                logger.info(f"Obtained {len(numeric_keys)} IDs in file")
                ids_to_delete[file_date.strftime("%Y-%m-%d")].extend(numeric_keys)
            else:
                logger.warn("No IDs in file!")

        total_ids = sum([len(v) for v in ids_to_delete.values()])
        logger.info(f"Total number of delete records to process: {total_ids}")
        return ids_to_delete

    def store_delete_records(self, id_list):
        logger.info("Nothing to store for procurement deletes")
