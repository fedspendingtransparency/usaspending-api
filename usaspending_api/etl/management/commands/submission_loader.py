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
            now = datetime.datetime.now()
            start = datetime.datetime(2017, 1, 1, 0, 0, 0)
            time_periods = create_full_time_periods(start, now, "q", {})
            all_periods = [(period["time_period"]["fy"], period["time_period"]["q"]) for period in time_periods]
            for fiscal_year_and_quarters in all_periods:
                fy = int(fiscal_year_and_quarters[0])
                q = int(fiscal_year_and_quarters[1])
                try:
                    logger.info("Running submission load for fy {}, quarter {}...".format(fy, q))
                    call_command("load_multiple_submissions", fy, q)
                except CommandError:
                    logger.exception("Error reported using fy/q combination: {} {}".format(fy, q))
                except SystemExit:
                    logger.info("Submission(s) errored in FY{} Q{}. Continuing...".format(fy, q))
                except Exception:
                    logger.exception("Submission(s) errored in FY{} Q{}".format(fy, q))
        elif options["ids"]:
            for idx in options["ids"]:
                logger.info("Running submission load for submission id {}".format(idx))
                try:
                    call_command("load_submission", "--noclean", idx)
                except CommandError:
                    logger.exception("Skipping submission ID {} due to CommandError (bad ID)".format(idx))
                except SystemExit:
                    logger.info("Skipping submission ID {} due to error. Continuing...".format(idx))
                except Exception:
                    logger.exception("Submission {} FAILED".format(idx))
