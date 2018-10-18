import logging

from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from django.db import connections

from usaspending_api.broker import lookups
from usaspending_api.broker.models import ExternalDataLoadDate
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

        parser.add_argument(
            '--date',
            dest="date",
            nargs=1,
            type=str,
            help="(OPTIONAL) Date from which to start the nightly loader. Expected format: YYYY-MM-DD"
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

        if options.get('date'):
            date = options.get('date')[0]
            try:
                date_conversion = datetime.strptime(date, '%Y-%m-%d')
                date = date_conversion.date()
            except ValueError:
                raise ValueError("Incorrect data format, should be YYYY-MM-DD")
        else:
            data_load_date_obj = ExternalDataLoadDate.objects. \
                filter(external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT['exec_comp']).first()
            if not data_load_date_obj:
                date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                date = data_load_date_obj.last_load_date
        start_date = datetime.utcnow().strftime('%Y-%m-%d')

        load_executive_compensation(db_cursor, date, start_date)
