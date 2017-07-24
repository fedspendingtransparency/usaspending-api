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

        potential_childless = set()
        for transaction in submission.transaction_set.all():
            self.logger.info('Running childless check on transaction => ' + str(transaction))
            potential_childless.add(transaction.place_of_performance)
            potential_childless.add(transaction.award.place_of_performance)
            if transaction.award.recipient:
                potential_childless.add(transaction.award.recipient.location)
            for subaward in transaction.award.subawards.all():
                potential_childless.add(subaward.place_of_performance)
                if subaward.recipient:
                    potential_childless.add(subaward.recipient.location)
            # could get the LegalEntities, too

        # Return without `None`s
        return potential_childless - {None}

    def handle(self, *args, **options):
        self.logger.info('Staring rm_submissions management command')

        # This will throw an exception and exit the command if the id doesn't exist
        try:
            submission = SubmissionAttributes.objects.get(broker_submission_id=options["submission_id"][0])
        except ObjectDoesNotExist:
            self.logger.error("Submission with broker id " + str(options["submission_id"][0]) + " does not exist")
            return

        self.logger.info('Getting childless submissions')
        potentially_childless = self.instances_potentially_left_childless(submission)

        deleted = defaultdict(int)
        deleted_total = 0
        deletion_tally = submission.delete()
        deleted_total += deletion_tally[0]
        deleted.update(deletion_tally[1])

        self.logger.info('Starting iteration over potentially childless submissions')
        for instance in potentially_childless:
            self.logger.info('Running deletion check on => ' + str(instance))
            deletions = instance.delete_if_childless()
            deleted_total += deletions[0]
            self.logger.info('Compiling deleted values for => ' + str(instance))
            for (key, value) in deletions[1].items():
                deleted[key] += value

        self.logger.info('Finished deletions')

        statistics = "Statistics:\n  Total objects Removed: {}".format(deleted_total)
        for (model, count) in deleted.items():
            statistics = statistics + "\n  {}: {}".format(model, count)

        self.logger.info("Deleted submission " + str(options["submission_id"][0]) + ". " + statistics)
