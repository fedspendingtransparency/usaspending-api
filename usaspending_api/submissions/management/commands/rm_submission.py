import logging
import signal

from django.core.exceptions import ObjectDoesNotExist
from django.core.management.base import BaseCommand
from django.db import transaction

from usaspending_api.submissions.models import SubmissionAttributes

logger = logging.getLogger("script")


class Command(BaseCommand):
    """Remove a DABS record and all associated data with it from the database"""

    help = "Removes a single submission from the database"

    def add_arguments(self, parser):
        parser.add_argument("submission_id", help="the Broker submission ID to delete", type=int)

    @transaction.atomic
    def handle(self, *args, **options):
        logger.info("Starting rm_submissions management command")

        def signal_handler(signal, frame):
            transaction.set_rollback(True)
            raise Exception("Received interrupt signal. Aborting...")

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        submission_id = options["submission_id"]

        try:
            submission = SubmissionAttributes.objects.get(submission_id=submission_id)
        except ObjectDoesNotExist:
            raise RuntimeError(f"Broker submission ID {submission_id} does not exist")

        deleted_stats = submission.delete()

        models = {
            "accounts.AppropriationAccountBalances": {"name": "File A", "count": 0},
            "financial_activities.FinancialAccountsByProgramActivityObjectClass": {"name": "File B", "count": 0},
            "awards.FinancialAccountsByAwards": {"name": "File C", "count": 0},
            "submissions.SubmissionAttributes": {"name": "Submission", "count": 0},
            "Total Rows": {"name": "DABS", "count": 0},
        }  # Using a Dict to set the logging order below

        for model, count in deleted_stats[1].items():
            models[str(model)]["count"] = count
            models["Total Rows"]["count"] += count

        if deleted_stats[0] != models["Total Rows"]["count"]:
            logger.error(f"Delete records mismatch!! Check for unknown FK relationships!")
            raise RuntimeError(f"ORM deletes {deleted_stats[0]:,} != expected {models['Total Rows']['count']:,}")

        statistics = "\n\t".join([f"{m} ({x['name']}): {x['count']:,}" for m, x in models.items()])
        logger.info(f"Deleted Broker submission ID {submission_id}:\n\t{statistics}")
        logger.info("Finished deletions by rm_submissions")
