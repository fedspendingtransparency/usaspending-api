from django.core.management.base import BaseCommand, CommandError
from usaspending_api.download.models import JobStatus
from usaspending_api.download import lookups

from django.db import transaction
import logging


@transaction.atomic
class Command(BaseCommand):
    help = "Loads agencies and sub-tier agencies from authoritative OMB list in \
            the folder of this management command."

    logger = logging.getLogger('console')

    @transaction.atomic
    def handle(self, *args, **options):

        for status in lookups.JOB_STATUS:
            job_status = JobStatus(job_status_id=status.id, name=status.name, description=status.desc)
            job_status.save()
