import logging

from django.core.management.base import BaseCommand
from django.db import connections, transaction

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.references.models import DABSSubmissionWindowSchedule 

logger = logging.getLogger("console")

SUBMISSION_WINDOW_SCHEDULE_SQL = """

"""


class Command(BaseCommand):
    help = "Update DABS Submission Window Schedule table based on Broker"

    @transaction.atomic()
    def handle(self, *args, **options):
        logger.info("Creating broker cursor")
        broker_cursor = connections["data_broker"].cursor()

        logger.info("Running SUBMISSION_WINDOW_SCHEDULE_SQL")
        broker_cursor.execute(SUBMISSION_WINDOW_SCHEDULE_SQL)

        logger.info("Getting total obligation values from cursor")
        submission_schedule_values = dictfetchall(broker_cursor)

        logger.info("Deleting existing DABS Submission Window Schedule")
        DABSSubmissionWindowSchedule.objects.all().delete()

        logger.info("Inserting DABS Submission Window Schedule into website")
        submission_schedule_objs = [DABSSubmissionWindowSchedule(**values) for values in submission_schedule_values]
        DABSSubmissionWindowSchedule.objects.bulk_create(submission_schedule_objs)

        logger.info("DABS Submission Window Schedule loader finished successfully!")
