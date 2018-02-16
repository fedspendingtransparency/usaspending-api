import os
import pytz

from datetime import datetime
from django.core.management.base import BaseCommand
from elasticsearch import Elasticsearch
from multiprocessing import Queue
from time import perf_counter

from usaspending_api import settings
from usaspending_api.etl.es_etl_helpers import AWARD_DESC_CATEGORIES
from usaspending_api.etl.es_etl_helpers import DataJob
from usaspending_api.etl.es_etl_helpers import download_db_records
from usaspending_api.etl.es_etl_helpers import es_data_loader
from usaspending_api.etl.es_etl_helpers import gather_deleted_ids

# SCRIPT OBJECTIVES and ORDER OF EXECUTION STEPS
# 1. [conditional] Remove recently deleted transactions from Elasticsearch
#   a. Gather the list of deleted transactions from S3
#   b. Delete transactions from ES
# 2. Iterate by fiscal year over award type description
#   a. Download 1 CSV file
#   b. Either delete target index or delete transactions by their ids
#   c. Load CSV contents into ES
#   d. Lather. Rinse. Repeat.

ES_CLIENT = Elasticsearch(settings.ES_HOSTNAME, timeout=300)


class Command(BaseCommand):
    help = '''
    '''

    # used by parent class
    def add_arguments(self, parser):
        parser.add_argument(
            'fiscal_year',
            type=int,
            help='If only one fiscal year is desired')
        parser.add_argument(
            '--since',
            default='2001-01-01',
            type=str,
            help='Start date for computing the delta of changed transactions [YYYY-MM-DD]')
        parser.add_argument(
            '--dir',
            default=os.path.dirname(os.path.abspath(__file__)),
            type=str,
            help='Set for a custom location of output files')
        parser.add_argument(
            '-d',
            '--deleted',
            action='store_true',
            help='Flag to include deleted transactions from S3')
        parser.add_argument(
            '-r',
            '--recreate',
            action='store_true',
            help='Flag to delete each ES index and recreate with new data')

    # used by parent class
    def handle(self, *args, **options):
        ''' Script execution of custom code starts in this method'''
        start = perf_counter()

        self.config = set_config()
        self.config['verbose'] = True if options['verbosity'] > 1 else False
        self.config['fiscal_year'] = options['fiscal_year']
        self.config['directory'] = options['dir'] + os.sep
        self.config['provide_deleted'] = options['deleted']
        self.config['recreate'] = options['recreate']
        self.config['starting_date'] = datetime.strptime(options['since'], '%Y-%m-%d')
        self.config['starting_date'] = datetime.combine(self.config['starting_date'], datetime.min.time(), tzinfo=pytz.UTC)

        if self.config['recreate'] and self.config['starting_date'] > datetime(2007, 1, 1):
            print('Bad mix of parameters! An index should not be dropped if only a subset of data will be loaded')
            raise SystemExit

        if not os.path.isdir(self.config['directory']):
            print('Provided directory does not exist')
            raise SystemExit

        self.controller()
        print('---------------------------------------------------------------')
        print("Script completed in {} seconds".format(perf_counter() - start))

    def controller(self):
        fetch_jobs = Queue()
        done_jobs = Queue()
        # Fetch the list of deleted records from S3 and delete them from ES
        self.deleted_ids = gather_deleted_ids(self.config) or {}
        # Future TODO: use this once ES has generated_unique_id columns
        # delete_list = [{'id': x, 'col': 'generated_unique_transaction_id'} for x in self.deleted_ids.keys()]
        # delete_transactions_from_es(delete_list)

        # Loop through award type categories
        for awd_cat_idx in AWARD_DESC_CATEGORIES.keys():
            # Download CSV to file
            award_category = AWARD_DESC_CATEGORIES[awd_cat_idx]
            filename = '{dir}{fy}_transactions_{type}.csv'.format(
                dir=self.config['directory'],
                fy=self.config['fiscal_year'],
                type=award_category)

            index = '{}-{}-{}'.format(
                settings.TRANSACTIONS_INDEX_ROOT,
                award_category,
                self.config['fiscal_year'])

            job = DataJob(None, index, self.config['fiscal_year'], awd_cat_idx, filename)
            fetch_jobs.put(job)
            # job.count = download_db_records(awd_cat_idx, self.config['fiscal_year'], filename)

        download_db_records(fetch_jobs, done_jobs, self.config)
        es_data_loader(ES_CLIENT, fetch_jobs, done_jobs, self.config)

        print('Completed all categories for FY{}'.format(self.config['fiscal_year']))


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
        'formatted_now': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),  # ISO8601
    }
