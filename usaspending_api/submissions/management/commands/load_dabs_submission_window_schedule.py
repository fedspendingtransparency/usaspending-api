import logging
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db import connections, transaction

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule
from usaspending_api.common.threaded_data_loader import ThreadedDataLoader

logger = logging.getLogger("script")

# SQL to create Month Period Schedules using broker table
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
    help = "Update DABS Submission Window Schedule table based on Broker"

    @transaction.atomic()
    def handle(self, *args, **options):

        logger.info("Deleting existing DABS Submission Window Schedule")
        DABSSubmissionWindowSchedule.objects.all().delete()

        if "data_broker" in connections:
            logger.info("Connection to broker exists. Generating schedule from broker.")
            self.generate_schedules_from_broker()
        else:
            logger.info("No connection to broker exists. Reading schedule from file.")
            self.read_schedules_from_csv()

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

        logger.info("Inserting DABS Submission Window Schedule into website")
        submission_schedule_objs = [DABSSubmissionWindowSchedule(**values) for values in month_schedule_values]
        submission_schedule_objs += [DABSSubmissionWindowSchedule(**values) for values in quarter_schedule_values]

        DABSSubmissionWindowSchedule.objects.bulk_create(submission_schedule_objs)

    def read_schedules_from_csv(self):
        project_root = Path(__name__).resolve().parent
        file_path = str(project_root / "usaspending_api" / "data" / "dabs_submission_window_schedule.csv")

        loader = ThreadedDataLoader(model_class=DABSSubmissionWindowSchedule, collision_behavior="update")
        loader.load_from_file(file_path)
