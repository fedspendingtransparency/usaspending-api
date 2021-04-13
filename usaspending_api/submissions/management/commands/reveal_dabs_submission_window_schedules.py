import logging
import pytz

from datetime import datetime
from django.core.management.base import BaseCommand
from django.db import transaction

from usaspending_api.submissions.models import DABSSubmissionWindowSchedule

logger = logging.getLogger("script")


class Command(BaseCommand):
    """
    This command will clear and repopulate the dabs_submission_window_schedule table.
    If a csv file is provided with the --file argument, rows will be created based on
    it. If no file is provided, rows will be generated based off of the broker. In
    production, the broker method should be used.
    """

    help = "Update DABS Submission Window Schedule table based on a file or the broker"

    @transaction.atomic()
    def handle(self, *args, **options):

        logger.info("Loading existing DABS Submission Window Schedules")
        existing_schedules = DABSSubmissionWindowSchedule.objects.all()

        now = datetime.now(tz=pytz.UTC)

        for schedule in existing_schedules:
            if schedule.submission_due_date < now and schedule.submission_reveal_date > now:
                schedule.submission_reveal_date = now
                schedule.save()
