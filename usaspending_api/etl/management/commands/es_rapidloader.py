
import json
import os

from datetime import date
from datetime import datetime
from django.core.management.base import BaseCommand
from elasticsearch import Elasticsearch
from multiprocessing import Process, Queue
from time import perf_counter, sleep
from usaspending_api import settings

from usaspending_api.etl.es_etl_helpers import AWARD_DESC_CATEGORIES
from usaspending_api.etl.es_etl_helpers import csv_row_count
from usaspending_api.etl.es_etl_helpers import DataJob
from usaspending_api.etl.es_etl_helpers import download_db_records
from usaspending_api.etl.es_etl_helpers import post_to_elasticsearch
from usaspending_api.etl.es_etl_helpers import printf


# SCRIPT OBJECTIVES and ORDER OF EXECUTION STEPS
# 1. Generate the full list of fiscal years and award descriptions to process as jobs
# 2. Iterate by job
#   a. Download 1 CSV file
#       i. Download the next CSV file until no more jobs need CSVs
#   b. Upload CSV to Elasticsearch
#       i. As a new CSV is ready, upload to ES
#       ii. delete CSV file
#   d. Lather. Rinse. Repeat.

ES_CLIENT = Elasticsearch(settings.ES_HOSTNAME, timeout=300)


class Command(BaseCommand):
    help = ''''''

    # used by parent class
    def add_arguments(self, parser):
        parser.add_argument(
            'fiscal_years',
            nargs='+',
            type=int)
        parser.add_argument(
            '--dir',
            default=os.path.dirname(os.path.abspath(__file__)),
            type=str,
            help='Set for a custom location of output files')
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

    # used by parent class
    def handle(self, *args, **options):
        ''' Script execution of custom code starts in this method'''
        start = perf_counter()
        printf({'msg': 'Starting script\n{}'.format('=' * 56)})

        self.config = set_config()
        self.config['formatted_now'] = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')  # ISO8601
        self.config['starting_date'] = date(2001, 1, 1)
        self.config['verbose'] = True if options['verbosity'] > 1 else False
        self.config['fiscal_years'] = options['fiscal_years']
        self.config['directory'] = options['dir'] + os.sep
        self.config['recreate'] = options['recreate']
        self.config['stale'] = options['stale']
        self.config['keep'] = options['keep']

        if not os.path.isdir(self.config['directory']):
            printf({'msg': 'Provided directory does not exist'})
            raise SystemExit

        self.controller()
        printf({'msg': '---------------------------------------------------------------'})
        printf({'msg': 'Script completed in {} seconds'.format(perf_counter() - start)})
        printf({'msg': '---------------------------------------------------------------'})

    def controller(self):

        download_queue = Queue()  # Queue for jobs whch need a csv downloaded
        es_ingest_queue = Queue()  # Queue for jobs which have a csv and are ready for ES ingest

        job_id = 0
        for fy in self.config['fiscal_years']:
            for awd_cat_idx in AWARD_DESC_CATEGORIES.keys():
                job_id += 1
                award_category = AWARD_DESC_CATEGORIES[awd_cat_idx]
                index = '{}-{}-{}'.format(settings.TRANSACTIONS_INDEX_ROOT, award_category, fy)
                filename = '{dir}{fy}_transactions_{type}.csv'.format(
                    dir=self.config['directory'],
                    fy=fy,
                    type=awd_cat_idx.replace(' ', ''))

                new_job = DataJob(job_id, index, fy, awd_cat_idx, filename)

                if os.path.exists(filename):
                    if self.config['stale']:
                        new_job.count = csv_row_count(filename)
                        printf({
                            'msg': 'Using existing file: {} | count {}'.format(filename, new_job.count),
                            'job': new_job.name,
                            'f': 'Download'})
                        es_ingest_queue.put(new_job)
                        continue
                    else:
                        os.remove(filename)
                download_queue.put(new_job)

        printf({'msg': 'There are {} jobs to process'.format(job_id)})

        download_proccess = Process(target=download_db_records, args=(download_queue, es_ingest_queue, self.config))
        es_index_process = Process(target=es_data_loader, args=(download_queue, es_ingest_queue, self.config))

        download_proccess.start()
        es_index_process.start()

        download_proccess.join()
        es_index_process.join()


def es_data_loader(fetch_jobs, done_jobs, config):
    while True:
        if not done_jobs.empty():
            job = done_jobs.get_nowait()
            if job.name is None:
                break

            printf({'msg': 'Starting new job', 'job': job.name, 'f': 'ES Ingest'})
            post_to_elasticsearch(ES_CLIENT, job, config)
            if os.path.exists(job.csv) and not config['keep']:
                os.remove(job.csv)
        else:
            printf({'msg': 'No Job :-( Sleeping 15s', 'f': 'ES Ingest'})
            sleep(15)

    printf({'msg': 'Completed Elasticsearch data load', 'f': 'ES Ingest'})
    return


def set_config():
    if not os.environ.get('DATABASE_URL'):
        print('Missing environment variable `DATABASE_URL`')
        raise SystemExit

    if not os.environ.get('ES_HOSTNAME'):
        print('Missing environment variable `ES_HOSTNAME`')
        raise SystemExit

    config = {}
    es_mapping_file = 'usaspending_api/etl/es_mapper.json'
    with open(es_mapping_file) as f:
        data = json.load(f)
        config['mapping'] = json.dumps(data)

    return config
