import logging
import pytz

from datetime import datetime
from django.core.management.base import BaseCommand
from django.db import transaction

from usaspending_api.submissions.models import DABSSubmissionWindowSchedule

logger = logging.getLogger("script")


class Command(BaseCommand):
    """
    This command updates the 'submission_reveal_date' field on dabs_submission_window_schedule
    table entries that are ready to be revealed. When the 'submission_due_date' for monthly
    schedules or the 'certification_deadline' for quarterly schedules is reached, the
    'submission_reveal_date' is set to the current time. This command is intended to be run
    after submissions finish loading in the nightly pipeline.
    """

    help = "Updates Reveal Dates in the DABS Submission Window Schedule table."

    @transaction.atomic()
    def handle(self, *args, **options):

        logger.info("Loading existing DABS Submission Window Schedules")
        existing_schedules = DABSSubmissionWindowSchedule.objects.all()

        now = datetime.now(tz=pytz.UTC)

        for schedule in existing_schedules:

            if schedule.is_quarter:
                incoming_submission_due_date = schedule.certification_due_date
            else:
                incoming_submission_due_date = schedule.submission_due_date

            if incoming_submission_due_date < now and schedule.submission_reveal_date > now:
                year = schedule.submission_fiscal_year
                month = schedule.submission_fiscal_month
                is_quarter = schedule.is_quarter
                logger.info(f"Revealing schedule {schedule.id} - Year {year}, Month: {month}, Quarter: {is_quarter}")

                schedule.submission_reveal_date = now
                schedule.save()
