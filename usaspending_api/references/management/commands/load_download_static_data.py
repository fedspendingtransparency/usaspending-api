from django.core.management.base import BaseCommand
from usaspending_api.download.models.download_job import JobStatus
from usaspending_api.download import lookups

from django.db import transaction
import logging


@transaction.atomic
class Command(BaseCommand):
    help = "Loads static enum data for job statuses."

    @transaction.atomic
    def handle(self, *args, **options):

        logger = logging.getLogger("script")
        JobStatus.objects.all().delete()

        for status in lookups.JOB_STATUS:
            logger.info("Updating status: {}".format(status))
            job_status = JobStatus(job_status_id=status.id, name=status.name, description=status.desc)
            job_status.save()
