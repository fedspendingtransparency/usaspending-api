import boto3
import csv
import logging
import re

from datetime import datetime
from django.conf import settings
from django.core.management.base import BaseCommand

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

    def handle(self, *args, **options):
        logger.info("STARTING SCRIPT")
        ids = self.determine_deleted_transactions(options["datetime"].date())
        import json

        logger.warn(json.dumps(ids))

    @staticmethod
    def determine_deleted_transactions(date):
        ids_to_delete = {}
        regex_str = ".*_delete_records_(IDV|award).*"

        if settings.IS_LOCAL:
            logger.info("Local mode does not obtain deleted records")
            return ids_to_delete

        if not (settings.USASPENDING_AWS_REGION and settings.FPDS_BUCKET_NAME):
            raise Exception("Missing required environment variables: USASPENDING_AWS_REGION, FPDS_BUCKET_NAME")

        # s3client = boto3.client("s3", region_name=settings.USASPENDING_AWS_REGION)
        # s3resource = boto3.resource("s3", region_name=settings.USASPENDING_AWS_REGION)
        # s3_bucket = s3resource.Bucket(settings.FPDS_BUCKET_NAME)
        s3 = boto3.resource("s3", region_name=settings.USASPENDING_AWS_REGION)
        s3_bucket = s3.Bucket(settings.FPDS_BUCKET_NAME)

        # Only use files that match the date we're currently checking
        for obj in s3_bucket.objects.all():
            key_name = obj.key
            if not re.search(regex_str, key_name):
                continue

            file_date = datetime.strptime(key_name[: key_name.find("_")], "%m-%d-%Y").date()

            if "/" in key_name or file_date < date:
                continue

            reader = list()
            logger.warn("========= {}".format(key_name))

            with open("filename", "r+b") as data:
                s3_bucket.download_fileobj(key_name, data)
                data.seek(0)
                logger.warn(data.read().decode("utf-8"))
                return
                reader = [d.split(",") for d in data.read().decode("utf-8").splitlines()]
                # data.seek(0)
                # reader = csv.reader(data.read().decode("utf-8").splitlines())

            # next(reader)  # skip the header

            for rows in reader:
                logger.warn(rows)
            unique_key_list = list([int(rows[0]) for rows in reader[1:]])

            if unique_key_list:
                ids_to_delete[file_date.strftime("%Y-%m-%d")] = unique_key_list

        total_ids = sum([len(v) for v in ids_to_delete.values()])
        logger.info("Number of records to delete: {}".format(total_ids))
        return ids_to_delete
