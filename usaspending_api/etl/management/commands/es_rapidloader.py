import os
import json

from datetime import datetime
from django.core.management.base import BaseCommand
from elasticsearch import Elasticsearch
from multiprocessing import Process, Queue
from time import perf_counter
from time import sleep

from usaspending_api import settings
from usaspending_api.etl.es_etl_helpers import AWARD_DESC_CATEGORIES
from usaspending_api.etl.es_etl_helpers import csv_row_count
from usaspending_api.etl.es_etl_helpers import DataJob
from usaspending_api.etl.es_etl_helpers import deleted_transactions
from usaspending_api.etl.es_etl_helpers import download_db_records
from usaspending_api.etl.es_etl_helpers import es_data_loader
from usaspending_api.etl.es_etl_helpers import swap_aliases
from usaspending_api.etl.es_etl_helpers import take_snapshot
from usaspending_api.etl.es_etl_helpers import printf
# SCRIPT OBJECTIVES and ORDER OF EXECUTION STEPS
# 1. Generate the full list of fiscal years and award descriptions to process as jobs
# 2. Iterate by job
#   a. Download 1 CSV file
#       i. Download the next CSV file until no more jobs need CSVs
#   b. Upload CSV to Elasticsearch
#       1. As a new CSV is ready, upload to ES
#       2. Either recreate index or remove existing docs with matching ids
#       3. [default] delete CSV file
#   d. Lather. Rinse. Repeat.

ES = Elasticsearch(settings.ES_HOSTNAME, timeout=300)


class Command(BaseCommand):
    help = ''''''

    # used by parent class
    def add_arguments(self, parser):
        parser.add_argument(
            'fiscal_years',
            nargs='+',
            type=int)
        parser.add_argument(
            '--since',
            default=None,
            type=str,
            help='Start date for computing the delta of changed transactions [YYYY-MM-DD]')
        parser.add_argument(
            '--dir',
            default=os.path.dirname(os.path.abspath(__file__)),
            type=str,
            help='Set for a custom location of output files')
        parser.add_argument(
            '--index_name',
            type=str,
            help='Set an index name to ingest data into',
            default='staging_transactions')
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
        parser.add_argument(
            '-s',
            '--stale',
            action='store_true',
            help='Flag allowed existing CSVs (if they exist) to be used instead of downloading new data')
        parser.add_argument(
            '-k',
            '--keep',
            action='store_true',
            help='CSV files are not deleted after they are uploaded')
        parser.add_argument(
            '-w',
            '--swap',
            action='store_true',
            help='Flag allowed to put aliases to index and close all indices with aliases associated')
        parser.add_argument(
            '--save',
            action='store_true',
            help='Flag allowed to put aliases to index and close all indices with aliases associated')

    # used by parent class
    def handle(self, *args, **options):
        ''' Script execution of custom code starts in this method'''
        start = perf_counter()
        printf({'msg': 'Starting script\n{}'.format('=' * 56)})

        self.config = set_config()
        self.config['verbose'] = True if options['verbosity'] > 1 else False
        self.config['fiscal_years'] = options['fiscal_years']
        self.config['directory'] = options['dir'] + os.sep
        self.config['provide_deleted'] = options['deleted']
        self.config['recreate'] = options['recreate']
        self.config['stale'] = options['stale']
        self.config['swap'] = options['swap']
        self.config['keep'] = options['keep']
        self.config['save'] = options['save']
        self.config['index_name'] = options['index_name']

        mappingfile = os.path.join(settings.BASE_DIR, 'usaspending_api/etl/es_transaction_mapping.json')
        with open(mappingfile) as f:
            mapping_dict = json.load(f)
            self.config['mapping'] = json.dumps(mapping_dict)
        self.config['doc_type'] = str(list(mapping_dict['mappings'].keys())[0])
        self.config['max_query_size'] = mapping_dict['settings']['index.max_result_window']

        if not options['since']:
            # Due to the queries used for fetching postgres data, `starting_date` needs to be present and a date
            #   before the earliest records in S3 and when Postgres records were updated.
            #   Choose the beginning of FY2008, and made it timezone-award for S3
            self.config['starting_date'] = datetime.strptime('2007-10-01+0000', '%Y-%m-%d%z')
        else:
            if self.config['recreate']:
                print('Bad mix of parameters! An index should not be dropped if only a subset of data will be loaded')
                raise SystemExit
            self.config['starting_date'] = datetime.strptime(options['since'] + '+0000', '%Y-%m-%d%z')

        if not os.path.isdir(self.config['directory']):
            printf({'msg': 'Provided directory does not exist'})
            raise SystemExit

        does_index_exist = ES.indices.exists(self.config['index_name'])

        if does_index_exist and self.config['recreate']:
            ES.indices.delete(self.config['index_name'])
            printf({'msg': 'Deleting index {}'.format(self.config['index_name'])})

        self.controller()
        printf({'msg': '---------------------------------------------------------------'})
        printf({'msg': 'Script completed in {} seconds'.format(perf_counter() - start)})
        printf({'msg': '---------------------------------------------------------------'})

    def controller(self):

        download_queue = Queue()  # Queue for jobs whch need a csv downloaded
        es_ingest_queue = Queue(10)  # Queue for jobs which have a csv and are ready for ES ingest
        job_id = 0
        for fy in self.config['fiscal_years']:
            for awd_cat_idx in AWARD_DESC_CATEGORIES.keys():
                job_id += 1
                index = self.config['index_name']
                filename = '{dir}{fy}_transactions_{type}.csv'.format(
                    dir=self.config['directory'],
                    fy=fy,
                    type=awd_cat_idx.replace(' ', ''))

                new_job = DataJob(job_id, index, fy, awd_cat_idx, filename)

                if os.path.exists(filename):
                    # This is mostly for testing. If previous CSVs still exist skip the download for that file
                    if self.config['stale']:
                        new_job.count = csv_row_count(filename)
                        printf({
                            'msg': 'Using existing file: {} | count {}'.format(filename, new_job.count),
                            'job': new_job.name,
                            'f': 'Download'})
                        # Add job directly to the Elasticsearch ingest queue since the CSV exists
                        es_ingest_queue.put(new_job)
                        continue
                    else:
                        os.remove(filename)
                download_queue.put(new_job)

        printf({'msg': 'There are {} jobs to process'.format(job_id)})

        if self.config['provide_deleted']:
            s3_delete_process = Process(target=deleted_transactions, args=(ES, self.config))
        download_proccess = Process(target=download_db_records, args=(download_queue, es_ingest_queue, self.config))
        es_index_process = Process(target=es_data_loader, args=(ES, download_queue, es_ingest_queue, self.config))

        download_proccess.start()

        if self.config['provide_deleted']:
            s3_delete_process.start()
            while s3_delete_process.is_alive():
                printf({'msg': 'Waiting to start ES ingest until S3 deletes are complete'})
                sleep(7)

        es_index_process.start()

        if self.config['provide_deleted']:
            s3_delete_process.join()
        download_proccess.join()
        es_index_process.join()

        if self.config['swap']:
            printf({'msg': 'Closing old indices and adding aliases'})
            swap_aliases(ES, self.config['index_name'])

        if self.config['save']:
            printf({'msg': 'Taking snapshot'})
            take_snapshot(ES, self.config['index_name'], settings.ES_REPOSITORY)


def set_config():
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
