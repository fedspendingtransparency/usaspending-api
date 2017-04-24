import logging

from django.core.management.base import BaseCommand
from django.conf import settings
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
            '-d',
            '--duns',
            dest="duns",
            nargs='+',
            type=int,
            help="DUNS to load subawards for"
        )

        parser.add_argument(
            '-a',
            '--all',
            action='store_true',
            dest='update_all',
            default=False,
            help='Update all DUNS present in the datastore',
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

        # Require users to explicitly call -a to use the "All" case so that
        # it is not accidentally triggered by omitting an option
        if options["duns"] is None and options["update_all"]:
            load_executive_compensation(db_cursor, None)
        elif options["duns"] is not None:
            load_executive_compensation(db_cursor, options["duns"])
        else:
            logger.info("Please specify either a list of DUNS (using the --duns) flag, or set to update all mode using the (--all) flag.")
