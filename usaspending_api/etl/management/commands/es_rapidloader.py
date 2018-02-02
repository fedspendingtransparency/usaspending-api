import json
import os
import pandas as pd
import subprocess

from datetime import date
from datetime import datetime
from django.core.management.base import BaseCommand
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from multiprocessing import Process, Queue
from time import perf_counter, sleep
from usaspending_api import settings
from usaspending_api.etl.management.commands.fetch_transactions import configure_sql_strings
from usaspending_api.etl.management.commands.fetch_transactions import execute_sql_statement
from usaspending_api.etl.management.commands.fetch_transactions import VIEW_COLUMNS

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


class DataJob:
    def __init__(self, *args, **kwargs):
        self._name = args[0]
        self.index = args[1] or kwargs['index']
        self.fy = args[2] or kwargs['fy']
        self.category = args[3] or kwargs['category']
        self.csv = args[4] or kwargs['csv']
        self.count = None


AWARD_DESC_CATEGORIES = {
    'loans': 'loans',
    'grant': 'grants',
    'insurance': 'other',
    'other': 'other',
    'contract': 'contracts',
    'direct payment': 'directpayments'
}


class Command(BaseCommand):
    help = '''
    '''

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
            help='Flag allowed existing CSVs to be used instead of downloading new data')

    # used by parent class
    def handle(self, *args, **options):
        ''' Script execution of custom code starts in this method'''
        start = perf_counter()
        print('\n\nStarting script\n')

        self.config = set_config()
        self.config['formatted_now'] = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')  # ISO8601
        self.config['verbose'] = True if options['verbosity'] > 1 else False
        self.config['fiscal_years'] = options['fiscal_years']
        self.config['directory'] = options['dir'] + os.sep
        self.config['recreate'] = options['recreate']
        self.config['stale'] = options['stale']

        self.config['starting_date'] = date(2001, 1, 1)

        if not os.path.isdir(self.config['directory']):
            print('Provided directory does not exist')
            raise SystemExit

        es_mapping_file = 'usaspending_api/etl/es_mapper.json'
        with open(es_mapping_file) as f:
            data = json.load(f)
            self.config['mapping'] = json.dumps(data)

        self.controller()
        print('---------------------------------------------------------------')
        print("Script completed in {} seconds".format(perf_counter() - start))
        print('---------------------------------------------------------------')

    def controller(self):

        new_queue = Queue()  # Queue for jobs whch need a csv downloaded
        ingest_queue = Queue()  # Queue for jobs which have a csv and are ready for ES ingest

        job_id = 0
        for fy in self.config['fiscal_years']:
            for awd_cat_idx in AWARD_DESC_CATEGORIES.keys():
                award_category = AWARD_DESC_CATEGORIES[awd_cat_idx]
                index = '{}-{}-{}'.format(settings.TRANSACTIONS_INDEX_ROOT, award_category, fy)
                filename = '{dir}{fy}_transactions_{type}.csv'.format(
                    dir=self.config['directory'],
                    fy=fy,
                    type=awd_cat_idx.replace(' ', ''))

                new_job = DataJob(job_id, index, fy, awd_cat_idx, filename)
                job_id += 1

                if self.config['stale'] is False:
                    if os.path.exists(filename):
                        os.remove(filename)
                    new_queue.put(new_job)
                else:
                    with open(filename, 'r') as f:
                        count = len(f.readlines()) - 1
                        new_job.count = count
                    ingest_queue.put(new_job)

        download_proccess = Process(target=download_db_records, args=(new_queue, ingest_queue, self.config))
        es_index_process = Process(target=es_data_loader, args=(new_queue, ingest_queue, self.config))

        download_proccess.start()
        es_index_process.start()

        download_proccess.join()
        es_index_process.join()


def es_data_loader(fetch_jobs, done_jobs, config):
    loop_again = True
    check_1 = True
    while loop_again:
        if not done_jobs.empty():
            print('[ES Ingest] Starting new job')
            check_1 = True
            job = done_jobs.get_nowait()
            post_to_elasticsearch(job, config)
            if os.path.exists(job.csv):
                os.remove(job.csv)
        elif fetch_jobs.empty() and done_jobs.empty():
            if check_1:
                print('[ES Ingest] Both Queues are empty. Check in 90s')
                check_1 = False
                sleep(90)
            else:
                loop_again = False
        else:
            print('[ES Ingest] No Job..... :-(')
            print('[ES Ingest] Sleeping 15s')
            sleep(15)

    print('[ES Ingest] Completed Elasticsearch data load')
    return


def download_csv(count_sql, copy_sql, filename, job_id, verbose):
    start = perf_counter()
    count = execute_sql_statement(count_sql, True, verbose)
    print('[Download {2}] Writing {0} transactions to this file: {1}'.format(count[0]['count'], filename, job_id))
    # It is preferable to not use shell=True, but this command works. Limited user-input so risk is low
    subprocess.Popen('psql "${{DATABASE_URL}}" -c {}'.format(copy_sql), shell=True).wait()
    ####################################
    # Potentially smart to perform basic validation that the number of CSV data rows match count
    print("[Download {1}] Database download took {0} seconds".format(perf_counter() - start, job_id))
    return count[0]['count']


def download_db_records(fetch_jobs, done_jobs, config):
    while not fetch_jobs.empty():
        start = perf_counter()
        job = fetch_jobs.get_nowait()
        print('[Download {}] Preparing to Download a new CSV'.format(job._name))

        c = {
            'starting_date': config['starting_date'],
            'fiscal_year': job.fy,
            'award_category': job.category,
            'provide_deleted': False
        }
        copy_sql, _, count_sql = configure_sql_strings(c, job.csv, [])

        if os.path.isfile(job.csv):
            os.remove(job.csv)

        job.count = download_csv(count_sql, copy_sql, job.csv, job._name, config['verbose'])
        done_jobs.put(job)
        print("[Download {1}] Data fetch job took {0} seconds.".format(perf_counter() - start, job._name))
        sleep(1)

    print('[Download] All downloads from Postgres completed')
    return


def set_config():
    if not os.environ.get('DATABASE_URL'):
        print('Missing environment variable `DATABASE_URL`')
        raise SystemExit

    if not os.environ.get('ES_HOSTNAME'):
        print('Missing environment variable `ES_HOSTNAME`')
        raise SystemExit

    return {}


def csv_chunk_gen(filename, chunksize, job_id):
    print('[ES Ingest {2}] Opening {0} to batch read by {1} lines'.format(filename, chunksize, job_id))
    # Panda's data type guessing causes issues for Elasticsearch. Set all cols to str
    dtype = {x: str for x in VIEW_COLUMNS}

    for file_df in pd.read_csv(filename, dtype=dtype, header=0, chunksize=chunksize):
        file_df = file_df.where(cond=(pd.notnull(file_df)), other=None)
        yield file_df.to_dict(orient='records')


def streaming_post_to_es(chunk, index_name, job_id):
    success, failed = 0, 0
    try:
        for ok, item in helpers.streaming_bulk(ES_CLIENT, chunk, index=index_name, doc_type='transaction_mapping'):
            success = [success, success + 1][ok]
            failed = [failed + 1, failed][ok]

    except Exception as e:
        print('MASSIVE FAIL!!!\n\n{}\n\n{}'.format(e, '*' * 80))
        raise SystemExit

    print('[ES Ingest {2}] Success: {0} | Fails: {1} '.format(success, failed, job_id))
    #
    return success, failed


def post_to_elasticsearch(job, config, chunksize=250000):
    print('[ES Ingest {1}] Populating ES Index : {0}'.format(job.index, job._name))

    try:
        does_index_exist = ES_CLIENT.indices.exists(job.index)
    except Exception as e:
        print(e)
        raise SystemExit
    if not does_index_exist:
        print('[ES Ingest {1}] Creating {0} index'.format(job.index))
        ES_CLIENT.indices.create(index=job.index, body=config['mapping'])
    elif does_index_exist and config['recreate']:
        print('[ES Ingest {1}] {0} exists... deleting first'.format(job.index, job._name))
        ES_CLIENT.indices.delete(job.index)

    csv_generator = csv_chunk_gen(job.csv, chunksize, job._name)
    for count, chunk in enumerate(csv_generator):
        print('[ES Ingest {1}] Running chunk # {0}'.format(count, job._name))
        iteration = perf_counter()
        streaming_post_to_es(chunk, job.index, job._name)
        print('[ES Ingest {1}] Iteration took {0}s'.format(perf_counter() - iteration, job._name))
