import logging
from datetime import datetime
import time

from django.db import connection
from django.core.management.base import BaseCommand, CommandError

from usaspending_api.etl.management.load_base import run_sql_file

logger = logging.getLogger('console')


class Command(BaseCommand):
    help = "Update specific FABS transactions and its related tables"
    pop_location = None
    legal_entity_location = None

    def add_arguments(self, parser):
        parser.add_argument(
            '--fiscal_year',
            type=int,
            help='Fiscal year to chose to pull from Broker'
        )

        parser.add_argument(
            '--assistance',
            action='store_true',
            dest='assistance',
            default=False,
            help='Updates FABS location data'
        )

        parser.add_argument(
            '--contracts',
            action='store_true',
            dest='contracts',
            default=False,
            help='Updates FPDS data'
        )

    def handle(self, *args, **options):
        query_parameters = {}
        start = datetime.now()

        fiscal_year = options.get('fiscal_year')

        if options.get('contracts', None):
            website_source = 'fpds'
        elif options.get('assistance', None):
            website_source = 'fabs'
        else:
            raise CommandError('Must specify --contracts or --assistance')

        if not fiscal_year:
            raise CommandError('Must specify --fiscal_year')

        query_parameters['fy_start'] = '10/01/' + str(fiscal_year - 1)
        query_parameters['fy_end'] = '09/30/' + str(fiscal_year)

        # Fetches rows that need to be updated based on batches pulled from the cursor
        logger.info('Fetching rows to update from broker for FY{} {} data'.format(fiscal_year, website_source.upper()))
        run_sql_file('usaspending_api/broker/management/commands/get_updated_{}_data.sql'.format(website_source), query_parameters)

        elapsed = datetime.now() - start
        # Retrieves temporary table with FABS rows that need to be updated
        start = datetime.now()
        db_cursor = connection.cursor()
        db_cursor.execute('SELECT count(*) from {}_transactions_to_update;'.format(website_source))
        db_rows = db_cursor.fetchall()[0][0]
        elapsed = datetime.now()-start

        logger.info("Completed fetching {} rows to update in {} seconds".format(db_rows, elapsed))

        start = datetime.now()
        if db_rows > 0:
            run_sql_file('usaspending_api/broker/management/commands/update_{}_location_data.sql'.format(website_source), {})

        elapsed = datetime.now() - start
        logger.info("Completed updating: {} {} rows in {} seconds".format(website_source.upper(), db_rows, elapsed))

        db_cursor.execute('DROP TABLE {}_transactions_to_update;'.format(website_source))
