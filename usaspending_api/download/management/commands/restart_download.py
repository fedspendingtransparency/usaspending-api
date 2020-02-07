import logging

from django.core.management.base import BaseCommand

from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.v2.download_admin import DownloadAdministrator

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = " ".join(
        [
            "Restart a 'frozen' download job.",
            "  (frozen signifies it didn't complete or fail, and is not a monthly download job)",
            "Provide a DownloadJob ID or filename to restart the download process.",
            "Depending on environment settings, this will either re-queue the download or process locally",
        ]
    )

    def add_arguments(self, parser):  # used by parent class
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "-i",
            "--id",
            dest="download_job_id",
            type=int,
            help="A DownloadJob ID from a USAspending DB to restart a download",
        )
        group.add_argument(
            "-f",
            "--filename",
            dest="file_name",
            type=str,
            help="A string to search on the final zip product filename of a DownloadJob to restart a download",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Throw caution into the wind and force that DownloadJob file generation to restart!",
        )

    def handle(self, *args, **options):  # used by parent class
        logger.info("Beginning management command")
        self.get_custom_arguments(**options)

        self.download = DownloadAdministrator()
        self.download.search_for_a_download(**self.get_custom_arguments(**options))
        if not options["force"]:
            self.validate_download_job()
        self.download.restart_download_operation()
        logger.info("OK")

    @staticmethod
    def get_custom_arguments(**options):
        return {field: value for field, value in options.items() if field in ["download_job_id", "file_name"] and value}

    def validate_download_job(self):
        if self.download.download_job.job_status_id in [JOB_STATUS_DICT["finished"], JOB_STATUS_DICT["failed"]]:
            report_and_exit(
                "DownloadJob invalid job_state_id: {}. Aborting".format(self.download.download_job.job_status_id)
            )
        elif self.download.download_job.monthly_download is True:
            report_and_exit("DownloadJob is a monthly download. Aborting")


def report_and_exit(log_message, exit_code=1):
    logger.error(log_message)
    raise SystemExit(exit_code)
