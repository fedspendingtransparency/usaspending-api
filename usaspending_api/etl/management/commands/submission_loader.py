import logging
import datetime

from django.core.management.base import BaseCommand, CommandError
from django.core.management import call_command

from usaspending_api.common.helpers.generic_helper import create_full_time_periods

logger = logging.getLogger("console")


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("--all-time", action="store_true", dest="all_time", help="all quarters and fiscal years.")
        parser.add_argument("--ids", nargs="+", help="list of ids to load", type=int)

    def handle(self, *args, **options):
        if options["all_time"]:
            self.run_for_all_fiscal_years()
        elif options["ids"]:
            self.run_specific_submissions(options["ids"])

    def run_for_all_fiscal_years(self):
        now = datetime.datetime.now()
        start = datetime.datetime(2017, 1, 1, 0, 0, 0)
        time_periods = create_full_time_periods(start, now, "q", {})
        all_periods = [(period["time_period"]["fy"], period["time_period"]["q"]) for period in time_periods]
        logger.info("Loading DABS Submissions from FY2017-Q2 to FY{}-{}".format(all_periods[-1][0], all_periods[-1][1]))
        for fiscal_year_and_quarters in all_periods:
            fy = int(fiscal_year_and_quarters[0])
            q = int(fiscal_year_and_quarters[1])
            start_msg = "Running submission load for FY{}-Q{}".format(fy, q)
            logger.info(start_msg)
            logger.info("=" * len(start_msg))
            try:
                call_command("load_multiple_submissions", fy, q)
            except SystemExit:
                logger.info("Submission(s) errors in FY{}-Q{}. Continuing...".format(fy, q))
            except (CommandError, Exception):
                logger.exception("Submission(s) errors in FY{}-Q{}. Continuing...".format(fy, q))

    def run_specific_submissions(self, list_of_submission_ids):
        logger.info("Attempting to load {} DABS Submissions".format(len(list_of_submission_ids)))
        for submission_id in list_of_submission_ids:
            logger.info("Running submission load for submission ID {}".format(submission_id))
            try:
                call_command("load_submission", submission_id)
            except SystemExit:
                logger.info("Skipping submission ID {} due to error. Continuing...".format(submission_id))
            except (CommandError, Exception):
                logger.exception("Skipping Submission ID {} due to error. Continuing...".format(submission_id))
