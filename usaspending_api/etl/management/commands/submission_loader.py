import logging
import datetime

from django.core.management.base import BaseCommand, CommandError
from django.core.management import call_command

from usaspending_api.common.helpers.generic_helper import create_full_time_periods
from usaspending_api.common.helpers.timing_helpers import Timer

logger = logging.getLogger("console")


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("--all-time", action="store_true", dest="all_time", help="all available FY+Q combinations")
        parser.add_argument("--ids", nargs="+", help="list of ids to load", type=int)

    def handle(self, *args, **options):
        self.failed_state = False
        if options["all_time"]:
            with Timer("Processing all available time periods"):
                self.run_for_all_fiscal_years()
        elif options["ids"]:
            with Timer("Processing list of submissions"):
                self.run_specific_submissions(options["ids"])
        else:
            logger.error("Nothing to do! use --all-time or --ids")
            return

        if self.failed_state:
            raise SystemExit("Submission errored. Returning failure exit code")

        logger.info("Completed Successfully")

    def run_for_all_fiscal_years(self):
        """Determine current FY+Q and run load_multiple_submissions from FY2017Q2 - current"""
        now = datetime.datetime.now()
        start = datetime.datetime(2017, 1, 1, 0, 0, 0)
        time_periods = create_full_time_periods(start, now, "q", {})
        all_periods = [(period["time_period"]["fy"], period["time_period"]["q"]) for period in time_periods]
        logger.info("Loading DABS Submissions from FY2017-Q2 to FY{}-{}".format(all_periods[-1][0], all_periods[-1][1]))
        for fiscal_year_and_quarters in all_periods:
            fy = int(fiscal_year_and_quarters[0])
            q = int(fiscal_year_and_quarters[1])
            start_msg = f"Running submission load for FY{fy}-Q{q}"
            logger.info(start_msg)
            logger.info("=" * len(start_msg))
            try:
                call_command("load_multiple_submissions", fy, q)
            except (CommandError, SystemExit, Exception):
                logger.exception(f"Submission(s) errors in FY{fy}-Q{q}. Continuing...")
                self.failed_state = True

    def run_specific_submissions(self, list_of_submission_ids):
        logger.info(f"Attempting to load {len(list_of_submission_ids)} DABS Submissions")
        for submission_id in list_of_submission_ids:
            logger.info(f"Running submission load for submission ID {submission_id}")
            try:
                call_command("load_submission", submission_id)
            except (CommandError, SystemExit, Exception):
                self.failed_state = True
                logger.exception(f"Skipping Submission ID {submission_id} due to error. Continuing...")
