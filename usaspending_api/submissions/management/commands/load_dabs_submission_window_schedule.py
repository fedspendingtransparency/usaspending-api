import logging

from django.core.management.base import BaseCommand
from django.db import connections, transaction

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule

logger = logging.getLogger("console")

# SQL to create Month Period Schedules using broker table
# Use all periods after Period 9, Year 2020 from table
# Submission Due Date comes from 'publish_deadline' column
MONTH_SCHEDULE_SQL = """
select
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
    period_start >= '2020-07-17'
"""

# SQL to create Quarter Period Schedules using broker table
# Only use periods 3, 6, 9, and 12 from table
# Submission Due Date comes from 'certification_deadline' column
QUARTER_SCHEDULE_SQL = """
select
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

# Manually creating some month period records because they are
# not represented in the broker table
EXTRA_MONTH_SCHEDULES = [
    DABSSubmissionWindowSchedule(
        period_start_date="2020-04-01 00:00:00",
        period_end_date="2020-04-30 00:00:00",
        submission_start_date="2020-07-17 00:00:00",
        submission_due_date="2020-07-30 00:00:00",
        certification_due_date="2020-08-14 00:00:00",
        submission_reveal_date="2020-07-31 00:00:00",
        submission_fiscal_year=2020,
        submission_fiscal_quarter=3,
        submission_fiscal_month=7,
        is_quarter=False,
    ),
    DABSSubmissionWindowSchedule(
        period_start_date="2020-05-01 00:00:00",
        period_end_date="2020-05-31 00:00:00",
        submission_start_date="2020-07-17 00:00:00",
        submission_due_date="2020-07-30 00:00:00",
        certification_due_date="2020-08-14 00:00:00",
        submission_reveal_date="2020-07-31 00:00:00",
        submission_fiscal_year=2020,
        submission_fiscal_quarter=3,
        submission_fiscal_month=8,
        is_quarter=False,
    ),
]


class Command(BaseCommand):
    help = "Update DABS Submission Window Schedule table based on Broker"

    @transaction.atomic()
    def handle(self, *args, **options):
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

        logger.info("Deleting existing DABS Submission Window Schedule")
        DABSSubmissionWindowSchedule.objects.all().delete()

        logger.info("Inserting DABS Submission Window Schedule into website")
        submission_schedule_objs = EXTRA_MONTH_SCHEDULES
        submission_schedule_objs += [DABSSubmissionWindowSchedule(**values) for values in month_schedule_values]
        submission_schedule_objs += [DABSSubmissionWindowSchedule(**values) for values in quarter_schedule_values]
        DABSSubmissionWindowSchedule.objects.bulk_create(submission_schedule_objs)

        logger.info("DABS Submission Window Schedule loader finished successfully!")
