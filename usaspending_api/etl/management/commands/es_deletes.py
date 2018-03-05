import os
import logging
from datetime import datetime
from django.core.management.base import BaseCommand
from elasticsearch import Elasticsearch

from usaspending_api import settings
from usaspending_api.etl.es_etl_helpers import filter_query
from usaspending_api.etl.es_etl_helpers import delete_query

logging.basicConfig(format='%(asctime)s |  %(message)s', level=logging.WARNING)



ES_CLIENT = Elasticsearch(settings.ES_HOSTNAME, timeout=300)


class Command(BaseCommand):
    help = '''
    '''

    # used by parent class
    def add_arguments(self, parser):
        parser.add_argument(
            '--table_name',
            type=int,
            help='Table name we are generating IDs from')
        parser.add_argument(
            '--column',
            default=None,
            type=str,
            help='Column that we are gathering deleted ids froms')
        parser.add_argument(
            '--index',
            default='future-transactions',
            type=str,
            help='Index prefix to be querying against')

    # used by parent class
    def handle(self, *args, **options):
        ''' Script execution of custom code starts in this method'''
        start = perf_counter()

        self.config = set_config()
        self.config['table_name'] = options['table_name']
        self.config['column'] = options['column']
        self.config['index'] = options['index']
        self.config['recreate'] = options['recreate']

        if not options['since']:
            # Due to the queries used for fetching postgres data, `starting_date` needs to be present and a date
            #   before the earliest records in S3 and when Postgres records were updated.
            #   Choose the beginning of FY2008, and made it timezone-award for S3
            self.config['starting_date'] = datetime.strptime('2007-10-01+0000', '%Y-%m-%d%z')
        else:
            if self.config['recreate']:
                logging.warn('Bad mix of parameters! An index should not be dropped if only a subset of data will be loaded')
                raise SystemExit
            self.config['starting_date'] = datetime.strptime(options['since'] + '+0000', '%Y-%m-%d%z')

        if not os.path.isdir(self.config['directory']):
            logging.warn('Provided directory does not exist')
            raise SystemExit

        self.controller()
        logging.warn('---------------------------------------------------------------')
        logging.warn("Script completed in {} seconds".format(perf_counter() - start))

    def controller(self):


        logging.warn('Completed all categories for FY{}'.format(self.config['fiscal_year']))


def set_config():
    if not os.environ.get('CSV_AWS_REGION'):
        print('Missing environment variable `CSV_AWS_REGION`')
        raise SystemExit

    if not os.environ.get('DELETED_TRANSACTIONS_S3_BUCKET_NAME'):
        print('Missing environment variable `DELETED_TRANSACTIONS_S3_BUCKET_NAME`')
        raise SystemExit

    if not os.environ.get('DATABASE_URL'):
        print('Missing environment variable `DATABASE_URL`')
        raise SystemExit

    if not os.environ.get('ES_HOSTNAME'):
        print('Missing environment variable `ES_HOSTNAME`')
        raise SystemExit

    return {
        'aws_region': os.environ.get('CSV_AWS_REGION'),
        's3_bucket': os.environ.get('DELETED_TRANSACTIONS_S3_BUCKET_NAME'),
        'root_index': settings.TRANSACTIONS_INDEX_ROOT,
        'formatted_now': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),  
    }
