from collections import defaultdict
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

    def instances_potentially_left_childless(self, submission):
        """All Location instances to check for delete-ability after this deletion.

        Could be expanded to other models.
        """

        potential_childless = []
        for transaction in submission.transaction_set.all():
            potential_childless.append(transaction.place_of_performance)
            potential_childless.append(transaction.award.place_of_performance)
            if transaction.award.recipient:
                potential_childless.append(transaction.award.recipient.location)
            for subaward in transaction.award.subawards.all():
                # potential_childless.append(subaward)
                potential_childless.append(subaward.place_of_performance)
                if subaward.recipient:
                    potential_childless.append(subaward.recipient.place_of_performance)
            # potential_childless.append(transaction.award)
            # could get the LegalEntities, too

        # Return without `None`s
        return [p for p in potential_childless if p]

    def handle(self, *args, **options):
        # This will throw an exception and exit the command if the id doesn't exist
        try:
            submission = SubmissionAttributes.objects.get(broker_submission_id=options["submission_id"][0])
        except ObjectDoesNotExist:
            self.logger.error("Submission with broker id " + str(options["submission_id"][0]) + " does not exist")
            return

        potentially_childless = self.instances_potentially_left_childless(submission)

        deleted = defaultdict(int)
        deleted_total = 0
        deletion_tally = submission.delete()
        deleted_total += deletion_tally[0]
        deleted.update(deletion_tally[1])
        for instance in potentially_childless:
            deletions = instance.delete_if_childless()
            deleted_total += deletions[0]
            for (key, value) in deletions[1].items():
                deleted[key] += value

        statistics = "Statistics:\n  Total objects Removed: {}".format(deleted_total)
        for (model, count) in deleted.items():
            statistics = statistics + "\n  {}: {}".format(model, count)

        self.logger.info("Deleted submission " + str(options["submission_id"][0]) + ". " + statistics)
