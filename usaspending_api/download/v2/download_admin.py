from django.conf import settings
from django.db.models import Q

from usaspending_api.common.sqs_helpers import get_sqs_queue_resource
from usaspending_api.download.filestreaming import csv_generation
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.models import DownloadJob


class DownloadAdministrator:
    def __init__(self):
        self.download_job = None

    def search_for_a_download(self, **kwargs):
        queryset_filter = self.craft_queryset_filter(**kwargs)
        self.get_download_job(queryset_filter)

    @staticmethod
    def craft_queryset_filter(**kwargs):
        if len(kwargs) == 0:
            raise Exception("An invalid value was provided to the argument")
        return Q(**kwargs)

    def get_download_job(self, queryset_filter):
        self.download_job = query_database_for_record(queryset_filter)

    def restart_download_operation(self):
        if process_is_local():
            self.update_download_job(job_status_id=JOB_STATUS_DICT["ready"], error_message=None)
            csv_generation.generate_csvs(download_job=self.download_job)
        else:
            self.push_job_to_queue()
            self.update_download_job(job_status_id=JOB_STATUS_DICT["queued"], error_message=None)

    def update_download_job(self, **kwargs):
        for field, value in kwargs.items():
            setattr(self.download_job, field, value)
        self.download_job.save()

    def push_job_to_queue(self):  # Candidate for separate object or file
        queue = get_sqs_queue_resource(queue_name=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME)
        queue.send_message(MessageBody=str(self.download_job.download_job_id))


def query_database_for_record(queryset_filter, raise_on_empty_result=True):  # Candidate for separate object or file
    result = None
    try:
        result = DownloadJob.objects.get(queryset_filter)
    except DownloadJob.DoesNotExist:
        if raise_on_empty_result:
            raise SystemExit("DownloadJob not found")
    return result


def process_is_local():  # Candidate for separate object or file
    return settings.IS_LOCAL
