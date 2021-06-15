import dateutil
import logging
import pytz
import time

from django.core.management.base import BaseCommand
from django.db.models import Q

from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.models.download_job import DownloadJob
from usaspending_api.download.v2.download_admin import DownloadAdministrator

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = " ".join(
        [
            "Restart all 'frozen' download jobs which meet the provided filters",
            "  (frozen signifies it didn't complete or fail, and is not a monthly download job)",
            "Ability to filter by an open or closed date range, job status, and if it is a monthly record",
            "Depending on environment settings, this will either re-queue the download or process locally",
        ]
    )

    def add_arguments(self, parser):  # used by parent class
        parser.add_argument(
            "-s",
            "--start",
            "--start-date",
            "--start-datetime",
            dest="start",
            type=dateutil.parser.parse,
            help="Earliest datetime to begin restarting downloads",
        )
        parser.add_argument(
            "-e",
            "--end",
            "--end-date",
            "--end-datetime",
            dest="end",
            type=dateutil.parser.parse,
            help="Most recent datetime to restart downloads",
        )
        parser.add_argument(
            "--status",
            dest="status",
            choices=[status for status in JOB_STATUS_DICT.keys() if status not in ("finished")],
            default="ready",
            help="Choose specific DownloadJob state to filter and process",
        )
        parser.add_argument(
            "--rate-limit",
            dest="rate-limit",
            type=int,
            default=60,
            help="Time delay in seconds between restarting jobs",
        )
        parser.add_argument(
            "--allow-monthly",
            dest="monthly",
            type=bool,
            default=False,
            help="If true: include and restart monthly download jobs",
        )

    def handle(self, *args, **options):  # used by parent class
        logger.info("Beginning management command")

        filters = self.parse_arguments_to_queryset_filter(**options)
        list_of_downloads = self.search_for_downloads(filters)
        logger.info("Found {} Download jobs to restart".format(len(list_of_downloads)))

        for download_job_id in list_of_downloads:
            logger.info("Sleeping {}s before restarting job #{}".format(options["rate-limit"], download_job_id))
            time.sleep(options["rate-limit"])

            download = DownloadAdministrator()
            download.search_for_a_download(download_job_id=download_job_id)
            download.restart_download_operation()

    def parse_arguments_to_queryset_filter(self, **kwargs):
        filter_set = Q(job_status=JOB_STATUS_DICT[kwargs["status"]])
        filter_set &= Q(monthly_download=kwargs["monthly"])
        if kwargs["start"]:
            filter_set &= Q(create_date__gte=kwargs["start"].replace(tzinfo=pytz.UTC))
        if kwargs["end"]:
            filter_set &= Q(create_date__lt=kwargs["end"].replace(tzinfo=pytz.UTC))

        return filter_set

    def search_for_downloads(self, filters):
        return DownloadJob.objects.filter(filters).order_by("create_date").values_list("download_job_id", flat=True)
