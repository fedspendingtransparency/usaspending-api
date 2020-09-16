import logging
import csv

from django.core.management.base import BaseCommand
from django.db import connections, transaction

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule
from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri

logger = logging.getLogger("script")

#  SQL to create Month Period Schedules using broker table
# Use all periods after Period 9, Year 2020 from table
# Submission Due Date comes from 'publish_deadline' column
MONTH_SCHEDULE_SQL = """
select
    year * 1000 + period * 10 + 0 as id,
    make_date(year, period, 1) - interval '3 months' as period_start_date,
    make_date(year, period, 1) - interval '2 months' - interval '1 day' as period_end_date,
    period_start as submission_start_date,
    certification_deadline as certification_due_date,
    publish_deadline as submission_due_date,
    publish_deadline + interval '1 day' as submission_reveal_date,
    year as submission_fiscal_year,
    (period + 2) / 3 as submission_fiscal_quarter,
    period as submission_fiscal_month,
    false as is_quarter
from
    submission_window_schedule
where
    period_start >= '2020-05-15'
    and period != 1
"""

# SQL to create Quarter Period Schedules using broker table
# Only use periods 3, 6, 9, and 12 from table
# Submission Due Date comes from 'certification_deadline' column
QUARTER_SCHEDULE_SQL = """
select
    year * 1000 + period * 10 + 1 as id,
    make_date(year, period, 1) - interval '5 months' as period_start_date,
    make_date(year, period, 1) - interval '2 months' - interval '1 day' as period_end_date,
    period_start as submission_start_date,
    certification_deadline as certification_due_date,
    certification_deadline as submission_due_date,
    certification_deadline + interval '1 day' as submission_reveal_date,
    year as submission_fiscal_year,
    (period + 2) / 3 as submission_fiscal_quarter,
    period as submission_fiscal_month,
    true as is_quarter
from
    submission_window_schedule
where
    period % 3 = 0
"""


class Command(BaseCommand):
    """
    This command will clear and repopulate the dabs_submission_window_schedule table.
    If a csv file is provided with the --file argument, rows will be created based on
    it. If no file is provided, rows will be generated based off of the broker. In
    production, the broker method should be used.
    """

    help = "Update DABS Submission Window Schedule table based on a file or the broker"

    def add_arguments(self, parser):
        parser.add_argument(
            "--file", help="The file containing schdules. If not provided, schedules are generated based on broker."
        )

    @transaction.atomic()
    def handle(self, *args, **options):

        file_path = options["file"]

        if file_path:
            logger.info("Input file provided. Reading schedule from file.")
            submission_schedule_objs = self.read_schedules_from_csv(file_path)
        else:
            logger.info("No input file provided. Generating schedule from broker.")
            submission_schedule_objs = self.generate_schedules_from_broker()

        logger.info("Deleting existing DABS Submission Window Schedule")
        DABSSubmissionWindowSchedule.objects.all().delete()

        logger.info("Inserting DABS Submission Window Schedule into website")
        DABSSubmissionWindowSchedule.objects.bulk_create(submission_schedule_objs)

        logger.info("DABS Submission Window Schedule loader finished successfully!")

    def generate_schedules_from_broker(self):

        logger.info("Creating broker cursor")
        broker_cursor = connections["data_broker"].cursor()

        logger.info("Running MONTH_SCHEDULE_SQL")
        broker_cursor.execute(MONTH_SCHEDULE_SQL)

        logger.info("Getting month schedule values from cursor")
        month_schedule_values = dictfetchall(broker_cursor)

        logger.info("Running QUARTER_SCHEDULE_SQL")
        broker_cursor.execute(QUARTER_SCHEDULE_SQL)

        logger.info("Getting quarter schedule values from cursor")
        quarter_schedule_values = dictfetchall(broker_cursor)

        submission_schedule_objs = [DABSSubmissionWindowSchedule(**values) for values in month_schedule_values]
        submission_schedule_objs += [DABSSubmissionWindowSchedule(**values) for values in quarter_schedule_values]

        return submission_schedule_objs

    def read_schedules_from_csv(self, file_path):

        logger.info("Reading from file: {}".format(file_path))

        with RetrieveFileFromUri(file_path).get_file_object(True) as file:
            csv_reader = csv.DictReader(file)
            submission_schedule_objs = [DABSSubmissionWindowSchedule(**values) for values in csv_reader]
            return submission_schedule_objs
