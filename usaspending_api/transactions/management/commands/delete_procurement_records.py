import boto3
import csv
import io
import logging
import re

from collections import defaultdict
from datetime import datetime
from django.conf import settings
from django.core.management.base import BaseCommand
from usaspending_api.common.helpers.sql_helpers import get_connection

from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type
from usaspending_api.transactions.generic_transaction_loader import GenericTransactionLoader
from usaspending_api.transactions.models.source_procurement_transaction import SourceProcurmentTransaction

logger = logging.getLogger("script")


class Command(GenericTransactionLoader, BaseCommand):
    help = "Upsert procurement transactions from a Broker database into an USAspending database"
    destination_table_name = SourceProcurmentTransaction().table_name
    shared_pk = "detached_award_procurement_id"

    def add_arguments(self, parser):
        parser.add_argument(
            "--date",
            dest="datetime",
            required=True,
            type=datetime_command_line_argument_type(naive=True),  # Broker and S3 date/times are naive.
            help="Load/Reload records from the provided datetime to the script execution start time.",
        )
        parser.add_argument(
            "--dry-run",
            dest="skip_deletes",
            action="store_true",
            help="Obtain the list of removed transactions, but skip the delete step.",
        )

    def handle(self, *args, **options):
        if not (settings.USASPENDING_AWS_REGION and settings.FPDS_BUCKET_NAME):
            raise Exception("Missing required environment variables: USASPENDING_AWS_REGION, FPDS_BUCKET_NAME")

        logger.info("STARTING SCRIPT")
        removed_records = self.fetch_deleted_transactions(options["datetime"].date())
        import json

        logger.warn(f"diagnostics: {json.dumps(removed_records)}")
        if removed_records and not options["skip_deletes"]:
            self.delete_rows(removed_records)
        else:
            logger.warn(f"Skipping deletes for {removed_records}")

    def delete_rows(self, removed_records):
        delete_template = "DELETE FROM {table} WHERE {key} IN {ids} AND updated_at < '{date}'::date"
        connection = get_connection(read_only=False)
        with connection.cursor() as cursor:
            for date, ids in removed_records.items():
                sql = delete_template.format(
                    table=self.destination_table_name, key=self.shared_pk, ids=tuple(ids), date=date
                )
                cursor.execute(sql)
                logger.info(f"Removed {cursor.rowcount} rows from {date} deletes")

    @staticmethod
    def fetch_deleted_transactions(date):
        ids_to_delete = defaultdict(list)
        regex_str = ".*_delete_records_(IDV|award).*"

        if settings.IS_LOCAL:
            logger.info("Local mode does not handle deleted records")
            return ids_to_delete

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

    @staticmethod
    def store_delete_records():
        logger.info("Nothing to store for procurement deletes")
