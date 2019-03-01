import botocore
import logging
from datetime import datetime, timezone

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
        parser.add_argument(
            "-i",
            "--id",
            default=None,
            type=int,
            help="Provide the DownloadJob ID from the USAspending DB to restart a download",
        )
        parser.add_argument(
            "-f",
            "--filename",
            default=None,
            type=str,
            help="Provide a filename to search by the final zip product name of a DownloadJob",
        )

    def handle(self, *args, **options):  # used by parent class
        download_job_id = search_downloads(options["id"], options["filename"])
        update_download_job(download_job_id, JOB_STATUS_DICT["ready"])


def search_downloads(dj_id, dj_file):
    queryset_filter = craft_queryset_filter(dj_id, dj_file)
    return get_download_job(queryset_filter)


def craft_queryset_filter(dj_id, dj_file):
    if dj_id:
        queryset_filter = Q(download_job_id=dj_id)
    elif dj_file:
        queryset_filter = Q(file_name=dj_file)
    else:
        raise Exception("Provide a download job ID or filename to search")
    return queryset_filter


def get_download_job(queryset_filter):
    download_job_id = query_database_for_record(queryset_filter)
    if not download_job_id:
        raise SystemExit(
            "Please verify your parameters. Either no DownloadJob found or record is in a prohibited state"
        )
    return download_job_id


def query_database_for_record(queryset_filter):
    return DownloadJob.objects.filter(queryset_filter).exclude(
        job_status_id__in=[JOB_STATUS_DICT["finished"], JOB_STATUS_DICT["failed"]], monthly_download=True
    ).values("download_job_id")


def update_download_job(djob_id, status):
    DownloadJob.objects.filter(download_job_id=djob_id).update(job_status_id=status, update_date=datetime.now(timezone.utc))


def push_job_to_queue(download_job_id):
    queue = get_sqs_queue_resource(queue_name=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME)
    queue.send_message(MessageBody=str(download_job_id))
