import logging

from django.core.management.base import BaseCommand
from django.db import connections, transaction

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule 

logger = logging.getLogger("console")

MONTH_SCHEDULE_SQL = """
select
    make_date(year, period, 1) - interval '3 months' as period_start,
    make_date(year, period, 1) - interval '2 months' - interval '1 day' as period_end,
    period_start as submission_start_date,
    certification_deadline as certification_due_date,
    publish_deadline as submission_due_date,
    publish_deadline + interval '1 day' as submission_reveal_date,
    year as submission_fiscal_year,
    (period + 2) / 3 as quarter,
    period as submission_fiscal_month,
    false as is_quarter
from
    submission_window_schedule
where
    period_start >= '2020-07-17'
"""


class Command(BaseCommand):
    help = "Update DABS Submission Window Schedule table based on Broker"

    @transaction.atomic()
    def handle(self, *args, **options):
        logger.info("Creating broker cursor")
        broker_cursor = connections["data_broker"].cursor()

        logger.info("Running SUBMISSION_WINDOW_SCHEDULE_SQL")
        broker_cursor.execute(MONTH_SCHEDULE_SQL)

        logger.info("Getting total obligation values from cursor")
        month_schedule_values = dictfetchall(broker_cursor)

        logger.info("Deleting existing DABS Submission Window Schedule")
        DABSSubmissionWindowSchedule.objects.all().delete()

        logger.info("Inserting DABS Submission Window Schedule into website")
        submission_schedule_objs = [DABSSubmissionWindowSchedule(**values) for values in month_schedule_values]
        DABSSubmissionWindowSchedule.objects.bulk_create(submission_schedule_objs)

        logger.info("DABS Submission Window Schedule loader finished successfully!")
