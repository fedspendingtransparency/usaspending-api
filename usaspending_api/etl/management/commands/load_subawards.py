import logging

from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import connections

from usaspending_api.etl.broker_etl_helpers import PhonyCursor
from usaspending_api.etl.subaward_etl import load_subawards

from usaspending_api.submissions.models import SubmissionAttributes

logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")


class Command(BaseCommand):
    """
    This command will load a specific submission id's subawards from the broker, or
    if the --all flag is specified, will load all submissions present in the datastore's
    file F records. The submission must already be present in the datastore.
    """
    help = "Loads a single submission's subawards from the configured data broker database"

    def add_arguments(self, parser):
        parser.add_argument(
            '-s',
            '--submission',
            dest="submission_id",
            nargs='+',
            type=int,
            help="Submission id to load subawards for"
        )

        parser.add_argument(
            '-a',
            '--all',
            action='store_true',
            dest='update_all',
            default=False,
            help='Update all submissions present in the datastore',
        )

        parser.add_argument(
            '-t',
            '--test',
            action='store_true',
            dest='test',
            default=False,
            help='Runs the submission loader in test mode, and uses stored data rather than pulling from a database'
        )

    def handle(self, *args, **options):
        # Grab the data broker database connections
        if not options['test']:
            try:
                db_conn = connections['data_broker']
                db_cursor = db_conn.cursor()
            except Exception as err:
                logger.critical('Could not connect to database. Is DATA_BROKER_DATABASE_URL set?')
                logger.critical(print(err))
                return
        else:
            db_cursor = PhonyCursor()

        submissions_to_update = []

        if options["update_all"]:
            submissions_to_update = SubmissionAttributes.objects.exclude(broker_submission_id__isnull=True)
        else:
            for submission_id in options['submission_id']:
                sub = SubmissionAttributes.objects.filter(broker_submission_id=submission_id).first()
                if not sub:
                    logger.critical("Submissions not found in datastore".format(options['submission_id']))
                else:
                    submissions_to_update.append(sub)

        for submission in submissions_to_update:
            try:
                logger.info("Loading subaward data for submission {}".format(submission.broker_submission_id))
                load_subawards(submission, db_cursor)
            except Exception as e:
                exception_logger.exception(e)
                logger.error("Loading subawards for submission {} failed. Exception has been logged.".format(submission.broker_submission_id))
