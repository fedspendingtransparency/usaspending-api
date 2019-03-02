import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Q

from usaspending_api.common.sqs_helpers import get_sqs_queue_resource
from usaspending_api.download.filestreaming import csv_generation
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.models import DownloadJob

logger = logging.getLogger("console")


class Command(BaseCommand):
    def add_arguments(self, parser):  # used by parent class
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "-i",
            "--id",
            dest="download_job_id",
            type=int,
            help="Provide the DownloadJob ID from the USAspending DB to restart a download",
        )
        group.add_argument(
            "-f",
            "--filename",
            dest="file_name",
            type=str,
            help="Provide a filename to search by the final zip product name of a DownloadJob to restart a download",
        )

    def handle(self, *args, **options):  # used by parent class
        logger.info("Beginning management command")
        self.search_downloads(**self.get_custom_arguments(**options))
        self.process_download_job()
        logger.info("OK\n")

    @staticmethod
    def get_custom_arguments(**options):
        return {field: value for field, value in options.items() if field in ["download_job_id", "file_name"] and value}

    def search_downloads(self, **kwargs):
        queryset_filter = self.craft_queryset_filter(**kwargs)
        self.get_download_job(queryset_filter)

    @staticmethod
    def craft_queryset_filter(**kwargs):
        if len(kwargs) == 0:
            report_and_exit("An invalid value was provided to the argument")
        return Q(**kwargs)

    def get_download_job(self, queryset_filter):
        self.download_job = query_database_for_record(queryset_filter)
        logger.info("DownloadJob record found")
        self.validate_download_job()

    def validate_download_job(self):
        if self.download_job.job_status_id in [JOB_STATUS_DICT["finished"], JOB_STATUS_DICT["failed"]]:
            report_and_exit("DownloadJob invalid job_state_id: {}. Aborting".format(self.download_job.job_status_id))
        elif self.download_job.monthly_download is True:
            report_and_exit("DownloadJob is a monthly download. Aborting")

    def process_download_job(self):
        if process_is_local():
            self.update_download_job(status=JOB_STATUS_DICT["ready"], error_message=None)
            csv_generation.generate_csvs(download_job=self.download_job)
        else:
            self.push_job_to_queue()
            self.update_download_job(status=JOB_STATUS_DICT["queued"], error_message=None)
            logger.info("DownloadJob successfully pushed to queue")

    def update_download_job(self, **kwargs):
        for field, value in kwargs.items():
            setattr(self.download_job, field, value)
        self.download_job.save()

    def push_job_to_queue(self):
        queue = get_sqs_queue_resource(queue_name=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME)
        queue.send_message(MessageBody=str(self.download_job.download_job_id))


def query_database_for_record(queryset_filter):
    try:
        return DownloadJob.objects.get(queryset_filter)
    except Exception as e:
        report_and_exit("DownloadJob not found")


def report_and_exit(msg):
    logger.error(msg)
    raise SystemExit(1)


def process_is_local():
    return settings.IS_LOCAL
