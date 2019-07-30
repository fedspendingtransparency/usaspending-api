import logging
import signal
from django.core.exceptions import ObjectDoesNotExist
from django.core.management.base import BaseCommand

from usaspending_api.submissions.models import SubmissionAttributes
from django.db import transaction


class Command(BaseCommand):
    """
    This command will remove a submission and all associated data with it from the
    database
    """

    help = "Removes a single submission from the configured data broker database"
    logger = logging.getLogger("console")

    def add_arguments(self, parser):
        parser.add_argument("submission_id", nargs=1, help="the broker submission id to delete", type=int)

    @transaction.atomic
    def handle(self, *args, **options):
        self.logger.info("Staring rm_submissions management command")

        def signal_handler(signal, frame):
            transaction.set_rollback(True)
            raise Exception("Received interrupt signal. Aborting...")

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        broker_submission_id = options["submission_id"][0]

        try:
            submission = SubmissionAttributes.objects.get(broker_submission_id=broker_submission_id)
        except ObjectDoesNotExist:
            raise "Broker submission id {} does not exist".format(broker_submission_id)

        deleted_stats = submission.delete()

        self.logger.info("Finished deletions.")

        statistics = "Statistics:\n  Total objects removed: {}".format(deleted_stats[0])
        for (model, count) in deleted_stats[1].items():
            statistics += "\n  {}: {}".format(model, count)

        self.logger.info("Deleted broker submission id {}. ".format(broker_submission_id) + statistics)
