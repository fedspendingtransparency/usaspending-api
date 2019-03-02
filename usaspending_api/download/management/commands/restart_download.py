from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Q

from usaspending_api.common.decorator_helpers import print_this
from usaspending_api.common.sqs_helpers import get_sqs_queue_resource
from usaspending_api.download.filestreaming import csv_generation
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.models import DownloadJob


class Command(BaseCommand):
    def add_arguments(self, parser):  # used by parent class
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "-i",
            "--id",
            default=None,
            type=int,
            help="Provide the DownloadJob ID from the USAspending DB to restart a download",
        )
        group.add_argument(
            "-f",
            "--filename",
            default=None,
            type=str,
            help="Provide a filename to search by the final zip product name of a DownloadJob to restart a download",
        )

    def handle(self, *args, **options):  # used by parent class
        download_job = search_downloads(options["id"], options["filename"])
        if process_download_job(download_job):
            print("OK")


@print_this
def search_downloads(dj_id, dj_file):
    queryset_filter = craft_queryset_filter(dj_id, dj_file)
    return get_download_job(queryset_filter)


@print_this
def craft_queryset_filter(dj_id, dj_file):
    if dj_id:
        queryset_filter = Q(download_job_id=dj_id)
    else:
        queryset_filter = Q(file_name=dj_file)
    return queryset_filter


@print_this
def get_download_job(queryset_filter):
    download_job = query_database_for_record(queryset_filter)
    validate_download_job(download_job)
    return download_job


@print_this
def query_database_for_record(queryset_filter):
    try:
        return DownloadJob.objects.get(queryset_filter)
    except Exception as e:
        raise SystemExit("DownloadJob not found")


@print_this
def validate_download_job(download_job):
    if download_job.job_status_id in [JOB_STATUS_DICT["finished"], JOB_STATUS_DICT["failed"]]:
        raise SystemExit("DownloadJob has invalid state id: {}".format(download_job.job_status_id))
    elif download_job.monthly_download is True:
        raise SystemExit("DownloadJob is a monthly download")


@print_this
def process_download_job(download_job):
    if process_is_local():
        update_download_job(download_job, status=JOB_STATUS_DICT["ready"], error_message=None)
        csv_generation.generate_csvs(download_job=download_job)
    else:
        push_job_to_queue(download_job.download_job_id)
        update_download_job(download_job, status=JOB_STATUS_DICT["queued"], error_message=None)
    return True


def process_is_local():
    return settings.IS_LOCAL


@print_this
def update_download_job(download_job, **kwargs):
    for field, value in kwargs.items():
        setattr(download_job, field, value)
    download_job.save()


@print_this
def push_job_to_queue(download_job_id):
    queue = get_sqs_queue_resource(queue_name=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME)
    queue.send_message(MessageBody=str(download_job_id))
