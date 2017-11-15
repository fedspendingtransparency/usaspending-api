import logging

from django.core.management.base import BaseCommand
from django.db import connections

from usaspending_api.etl.broker_etl_helpers import PhonyCursor
from usaspending_api.etl.executive_compensation_etl import load_executive_compensation

logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")


class Command(BaseCommand):
    """
    This command loads the executive compensation data from the broker using
    either a list of DUNS or all duns present in the data store
    """
    help = "Loads a set of DUNS's executive compensation data from the configured data broker database"

    def add_arguments(self, parser):

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

        load_executive_compensation(db_cursor)
