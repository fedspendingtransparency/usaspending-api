from datetime import datetime
import logging
import os
from django.core.management.base import BaseCommand
from django.core.exceptions import ObjectDoesNotExist

from usaspending_api.submissions.models import SubmissionAttributes


class Command(BaseCommand):
    """
    This command will remove a submission and all associated data with it from the
    database
    """
    help = "Removes a single submission from the configured data broker database"
    logger = logging.getLogger('console')

    def add_arguments(self, parser):
        parser.add_argument('submission_id', nargs=1, help='the broker submission id to delete', type=int)

    def handle(self, *args, **options):
        # This will throw an exception and exit the command if the id doesn't exist
        try:
            submission = SubmissionAttributes.objects.get(broker_submission_id=options["submission_id"][0])
        except ObjectDoesNotExist:
            self.logger.error("Submission with broker id " + str(options["submission_id"][0]) + " does not exist")
            return

        deleted = submission.delete()

        statistics = "Statistics:\n  Total objects Removed: " + str(deleted[0])
        for model in deleted[1].keys():
            statistics = statistics + "\n  " + model + ": " + str(deleted[1][model])

        self.logger.info("Deleted submission " + str(options["submission_id"][0]) + ". " + statistics)
