import logging
import pytz

from datetime import datetime
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db import connections

logger = logging.getLogger("console")
exception_logger = logging.getLogger("exceptions")


class Command(BaseCommand):

    # Give it a fiscal year and a quarter. Will list missing subs/agencies and their recent certified dates.
    def add_arguments(self, parser):
        parser.add_argument("fy", nargs=1, help="the fiscal year", type=int)
        parser.add_argument("quarter", nargs=1, help="the fiscal quarter to load", type=int)
        parser.add_argument("--safe", action="store_true", help="only list missing submissions from the FY/Quarter")

    def handle(self, *args, **options):

        try:
            broker_conn = connections["data_broker"]
            broker_cursor = broker_conn.cursor()
            api_conn = connections["default"]
            api_cursor = api_conn.cursor()
        except Exception as err:
            logger.critical("Could not connect to database(s).")
            logger.critical(err)
            return

        fy = options["fy"][0]
        quarter = options["quarter"][0]

        if not 1 <= quarter <= 4:
            logger.critical("Acceptable values for fiscal quarter are 1-4 (was {}).".format(quarter))
            return

        # Convert fiscal quarter to starting month of calendar quarter
        quarter = int(quarter) * 3

        broker_cursor.execute(
            "SELECT submission.submission_id, MAX(certify_history.created_at) AS certified_at, \
                                  submission.cgac_code, submission.frec_code \
                                  FROM submission \
                                  JOIN certify_history ON certify_history.submission_id = submission.submission_id \
                                  WHERE submission.d2_submission = FALSE \
                                  AND submission.publish_status_id IN (2, 3) \
                                  AND submission.reporting_fiscal_year = {} \
                                  AND submission.reporting_fiscal_period = {} \
                                  GROUP BY submission.submission_id;".format(
                fy, quarter
            )
        )

        broker_submission_data = broker_cursor.fetchall()

        missing_submissions = []
        failed_submissions = []
        for next_broker_sub in broker_submission_data:
            submission_id = next_broker_sub[0]
            try:
                certify_date = next_broker_sub[1].replace(tzinfo=pytz.UTC)
                cgac = next_broker_sub[2]
                frec = next_broker_sub[3]

                api_cursor.execute(
                    "SELECT update_date \
                                FROM submission_attributes \
                                WHERE broker_submission_id = {}".format(
                        submission_id
                    )
                )

                if api_cursor.rowcount:
                    most_recently_loaded_date = api_cursor.fetchone()[0].replace(tzinfo=pytz.UTC)
                else:
                    most_recently_loaded_date = datetime(2000, 1, 1).replace(tzinfo=pytz.UTC)

                if frec:
                    broker_cursor.execute("SELECT agency_name FROM frec WHERE frec_code = '{}'".format(frec))
                else:
                    broker_cursor.execute("SELECT agency_name FROM cgac WHERE cgac_code = '{}'".format(cgac))

                agency_name = broker_cursor.fetchone()[0]

                if certify_date > most_recently_loaded_date:
                    missing_submissions.append((submission_id, agency_name, certify_date, most_recently_loaded_date))
            except Exception as error:
                logger.exception("Submission {} failed in pull from broker: {}".format(submission_id, error))
                failed_submissions.append(str(submission_id))
        logger.info("Total missing submissions: {}".format(len(missing_submissions)))
        logger.info("-----------------------------------")
        for next_missing_sub in missing_submissions:
            logger.info(
                "Submission ID {} ({})\tCertified: {}".format(
                    next_missing_sub[0], next_missing_sub[1], next_missing_sub[2].date()
                )
            )
        logger.info("-----------------------------------")

        # Data modification happens here, if you don't flag '--safe'
        # The submission loader is atomic, so one of these failing should not affect subsequent submissions
        if not options["safe"]:
            for next_missing_sub in missing_submissions:
                submission_id = next_missing_sub[0]
                try:
                    call_command("load_submission", "--noclean", submission_id)
                except Exception as error:
                    logger.exception("Submission {} failed to load: {}".format(submission_id, error))
                    failed_submissions.append(str(submission_id))

        # If there were any failures, display them
        if failed_submissions:
            logger.error("The following submissions failed: {}".format(", ".join(failed_submissions)))
            exit(3)
