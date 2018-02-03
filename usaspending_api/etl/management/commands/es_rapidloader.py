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
    def __init__(self, *args):
        self.name = args[0]
        self.index = args[1]
        self.fy = args[2]
        self.category = args[3]
        self.csv = args[4]
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
        printf({'msg': 'Starting script\n{}'.format('=' * 56)})

        self.config = set_config()
        self.config['formatted_now'] = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')  # ISO8601
        self.config['verbose'] = True if options['verbosity'] > 1 else False
        self.config['fiscal_years'] = options['fiscal_years']
        self.config['directory'] = options['dir'] + os.sep
        self.config['recreate'] = options['recreate']
        self.config['stale'] = options['stale']

        self.config['starting_date'] = date(2001, 1, 1)

        if not os.path.isdir(self.config['directory']):
            printf({'msg': 'Provided directory does not exist'})
            raise SystemExit

        self.controller()
        printf({'msg': '---------------------------------------------------------------'})
        printf({'msg': 'Script completed in {} seconds'.format(perf_counter() - start)})
        printf({'msg': '---------------------------------------------------------------'})

    def controller(self):

        new_queue = Queue()  # Queue for jobs whch need a csv downloaded
        ingest_queue = Queue()  # Queue for jobs which have a csv and are ready for ES ingest

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

                if self.config['stale'] is False:
                    if os.path.exists(filename):
                        os.remove(filename)
                    new_queue.put(new_job)
                else:
                    with open(filename, 'r') as f:
                        count = len(f.readlines()) - 1
                        new_job.count = count
                    ingest_queue.put(new_job)

        printf({'msg': 'There are {} jobs to process'.format(job_id)})

        download_proccess = Process(target=download_db_records, args=(new_queue, ingest_queue, self.config))
        es_index_process = Process(target=es_data_loader, args=(new_queue, ingest_queue, self.config))

        download_proccess.start()
        es_index_process.start()

        download_proccess.join()
        es_index_process.join()


def es_data_loader(fetch_jobs, done_jobs, config):
    loop_again = True
    while loop_again:
        if not done_jobs.empty():
            job = done_jobs.get_nowait()
            if job.name is None:
                loop_again = False
                break

            printf({'msg': 'Starting new job', 'job': job.name, 'f': 'ES Ingest'})
            post_to_elasticsearch(job, config)
            if os.path.exists(job.csv):
                os.remove(job.csv)
        # elif fetch_jobs.empty() and done_jobs.empty():
        else:
            printf({'msg': 'No Job :-( Sleeping 15s', 'f': 'ES Ingest'})
            sleep(15)

    printf({'msg': 'Completed Elasticsearch data load', 'f': 'ES Ingest'})
    return


def download_csv(count_sql, copy_sql, filename, job_id, verbose):
    count = execute_sql_statement(count_sql, True, verbose)[0]['count']
    printf({'msg': 'Writing {} transactions to this file: {}'.format(count, filename), 'job': job_id, 'f': 'Download'})
    # It is preferable to not use shell=True, but this command works. Limited user-input so risk is low
    subprocess.Popen('psql "${{DATABASE_URL}}" -c {}'.format(copy_sql), shell=True).wait()

    # with open(filename, 'r') as f:
    #     download_count = len(f.readlines()) - 1
    # if count != download_count:
    #     printf({'msg': 'download_count {} in this file: {}'.format(download_count, filename), 'job': job_id, 'f': 'Download'})
    #     print('Download count doesn\'t match rows in DB!')
    #     raise SystemExit
    return count


def download_db_records(fetch_jobs, done_jobs, config):
    while not fetch_jobs.empty():
        start = perf_counter()
        job = fetch_jobs.get_nowait()
        printf({'msg': 'Preparing to Download a new CSV', 'job': job.name, 'f': 'Download'})

        sql_config = {
            'starting_date': config['starting_date'],
            'fiscal_year': job.fy,
            'award_category': job.category,
            'provide_deleted': False
        }
        copy_sql, _, count_sql = configure_sql_strings(sql_config, job.csv, [])

        if os.path.isfile(job.csv):
            os.remove(job.csv)

        job.count = download_csv(count_sql, copy_sql, job.csv, job.name, config['verbose'])
        done_jobs.put(job)
        printf({
            'msg': 'Data fetch job took {} seconds'.format(perf_counter() - start),
            'job': job.name,
            'f': 'Download'
        })
        sleep(1)

    done_jobs.put(DataJob(None, None, None, None, None))
    printf({'msg': 'All downloads from Postgres completed', 'f': 'Download'})
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


def csv_chunk_gen(filename, chunksize, job_id):
    printf({'msg': 'Opening {} batch size: {}'.format(filename, chunksize), 'job': job_id, 'f': 'ES Ingest'})
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

    printf({'msg': 'Success: {} | Fails: {}'.format(success, failed), 'job': job_id, 'f': 'ES Ingest'})
    return success, failed


def post_to_elasticsearch(job, config, chunksize=250000):
    printf({'msg': 'Populating ES Index : {}'.format(job.index), 'job': job.name, 'f': 'ES Ingest'})

    try:
        does_index_exist = ES_CLIENT.indices.exists(job.index)
    except Exception as e:
        print(e)
        raise SystemExit
    if not does_index_exist:
        printf({'msg': 'Creating {} index'.format(job.index), 'job': job.name, 'f': 'ES Ingest'})
        ES_CLIENT.indices.create(index=job.index, body=config['mapping'])
    elif does_index_exist and config['recreate']:
        printf({'msg': 'Deleting Existing index {}'.format(job.index), 'job': job.name, 'f': 'ES Ingest'})
        ES_CLIENT.indices.delete(job.index)

    csv_generator = csv_chunk_gen(job.csv, chunksize, job.name)
    for count, chunk in enumerate(csv_generator):
        iteration = perf_counter()
        streaming_post_to_es(chunk, job.index, job.name)
        printf({
            'msg': 'Iteration on chunk {} took {}s'.format(count, perf_counter() - iteration),
            'job': job.name,
            'f': 'ES Ingest'
        })


def printf(items):
    template = '[{time}] {complex:<20} | {msg}'
    msg = items['msg']
    func = '[' + items.get('f', 'main') + ']'
    job = items.get('job', None)
    j = ''
    if job:
        j = ' (#{})'.format(job)

    print(template.format(time=datetime.utcnow().strftime('%H:%M:%S.%f'), complex=func + j, msg=msg))
