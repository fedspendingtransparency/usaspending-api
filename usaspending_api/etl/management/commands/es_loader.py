import json
import os
import pandas as pd
import subprocess
from collections import defaultdict

from datetime import date
from datetime import datetime
from django.core.management.base import BaseCommand
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from time import perf_counter
from usaspending_api import settings
from usaspending_api.etl.management.commands.fetch_transactions import configure_sql_strings
from usaspending_api.etl.management.commands.fetch_transactions import DROP_VIEW_SQL
from usaspending_api.etl.management.commands.fetch_transactions import execute_sql_statement
from usaspending_api.etl.management.commands.fetch_transactions import gather_deleted_ids
from usaspending_api.etl.management.commands.fetch_transactions import TEMP_ES_DELTA_VIEW
from usaspending_api.etl.management.commands.fetch_transactions import VIEW_COLUMNS

# SCRIPT OBJECTIVES and ORDER OF EXECUTION STEPS
# 1. [conditional] Remove deleted transactions from ES
#   a. Gather the list of deleted transactions from S3
#   b. Delete transactions from ES
# 2. Create temporary view of transaction records
# 3. Iterate over fiscal year and award type description
#   a. Download 1 CSV file
#   b. Either delete target index or delete transactions by their ids
#   c. Load CSV contents into ES
#   d. Lather. Rinse. Repeat. until all fiscal years are complete

ES_CLIENT = Elasticsearch(settings.ES_HOSTNAME, timeout=300)

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
            '-w',
            '--wipe',
            action='store_true',
            help='Flag to purge ES indicies and recreate')

    # used by parent class
    def handle(self, *args, **options):
        ''' Script execution of custom code starts in this method'''
        start = perf_counter()

        self.config = set_config()
        self.config['formatted_now'] = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')  # ISO8601
        self.config['verbose'] = True if options['verbosity'] > 1 else False
        self.config['fiscal_year'] = options['fiscal_year']
        self.config['directory'] = options['dir'] + os.sep
        self.config['provide_deleted'] = options['deleted']
        self.config['wipe_indicies'] = options['wipe']

        try:
            self.config['starting_date'] = date(*[int(x) for x in options['since'].split('-')])
        except Exception:
            print('Malformed date string provided. `--since` requires YYYY-MM-DD')
            raise SystemExit

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

    def controller(self):
        # Fetch the list of deleted records from S3 and delete them from ES
        self.deleted_ids = gather_deleted_ids(self.config) or {}
        # Future TODO: use this once ES has generated_unique_id columns
        # delete_list = [{'id': x, 'col': 'generated_unique_transaction_id'} for x in self.deleted_ids.keys()]
        # delete_transactions_from_es(delete_list)

        ########################################################################
        # self.prepare_db()  # REMOVED for READONLY account
        ########################################################################

        # Loop through award type categories
        for awd_cat_idx in AWARD_DESC_CATEGORIES.keys():
            loop_msg = 'Handeling {} transactions for FY{}'.format(awd_cat_idx, self.config['fiscal_year'])
            print('{1}\n{0}'.format(loop_msg, '+' * len(loop_msg)))

            # Download CSV to file
            award_category = AWARD_DESC_CATEGORIES[awd_cat_idx]
            filename = '{dir}{fy}_transactions_{type}.csv'.format(
                dir=self.config['directory'],
                fy=self.config['fiscal_year'],
                type=award_category)

            count = self.download_db_records(awd_cat_idx, self.config['fiscal_year'], filename)

            index = '{}-{}-{}'.format(
                settings.TRANSACTIONS_INDEX_ROOT,
                award_category,
                self.config['fiscal_year'])
            job = {'file': filename, 'index': index, 'count': count, 'wipe': self.config['wipe_indicies']}

            # Upload CSV to ES
            post_to_elasticsearch(job, self.config['mapping'])
            # Delete File
            os.remove(filename)
            # repeat

        print('Completed all categories for FY{}'.format(self.config['fiscal_year']))
        ########################################################################
        # self.cleanup_db()  # REMOVED for READONLY account
        ########################################################################

    def prepare_db(self):
        print('Creating View in Postgres...')
        # REMOVED for READONLY account
        execute_sql_statement(TEMP_ES_DELTA_VIEW, False, self.config['verbose'])
        print('View Successfully created')

    def cleanup_db(self):
        print('Removing View from Postgres...')
        execute_sql_statement(DROP_VIEW_SQL, False, self.config['verbose'])
        print('View Successfully removed')

    def download_csv(self, count_sql, copy_sql, filename):
        start = perf_counter()
        count = execute_sql_statement(count_sql, True, self.config['verbose'])
        print('\nWriting {} transactions to this file:'.format(count[0]['count']))
        print(filename)
        # It is preferable to not use shell=True, but this command works. Limited user-input so risk is low
        subprocess.Popen('psql "${{DATABASE_URL}}" -c {}'.format(copy_sql), shell=True).wait()
        ####################################
        # Potentially smart to perform basic validation that the number of CSV data rows match count
        print("Database download took {} seconds".format(perf_counter() - start))
        return count[0]['count']

    def download_db_records(self, award_category, fiscal_year, filename):
        print('---------------------------------------------------------------')
        print('Preparing to Download a CSV')

        config = {
            'starting_date': self.config['starting_date'],
            'fiscal_year': fiscal_year,
            'award_category': award_category,
            'provide_deleted': self.config['provide_deleted']
        }
        _, copy_sql, _, count_sql, _ = configure_sql_strings(config, filename, [])

        if os.path.isfile(filename):
            os.remove(filename)

        count = self.download_csv(count_sql, copy_sql, filename)
        return count


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

    config = {
        'aws_region': os.environ.get('CSV_AWS_REGION'),
        's3_bucket': os.environ.get('DELETED_TRANSACTIONS_S3_BUCKET_NAME')
    }

    return config


def csv_chunk_gen(filename, chunksize):
    print('Opening {} to batch read by {} lines'.format(filename, chunksize))
    # Panda's data type guessing causes issues for Elasticsearch. Set all cols to str
    dtype = {x: str for x in VIEW_COLUMNS}

    for file_df in pd.read_csv(filename, dtype=dtype, header=0, chunksize=chunksize):
        file_df = file_df.where(cond=(pd.notnull(file_df)), other=None)
        yield file_df.to_dict(orient='records')


def streaming_post_to_es(chunk, index_name):
    success, failed = 0, 0
    try:
        for ok, item in helpers.streaming_bulk(ES_CLIENT, chunk, index=index_name, doc_type='custom_mapping'):
            success = [success, success + 1][ok]
            failed = [failed + 1, failed][ok]

    except Exception as e:
        print('MASSIVE FAIL!!!\n\n{}\n\n{}'.format(e, '*' * 80))
        raise SystemExit

    print('Success: {} | Fails: {} '.format(success, failed))
    #
    return success, failed


def post_to_elasticsearch(job, mapping, chunksize=250000):
    print('---------------------------------------------------------------')
    print('Populating ES Index : {}'.format(job['index']))

    does_index_exist = ES_CLIENT.indices.exists(job['index'])
    if not does_index_exist:
        print('Creating {} index'.format(job['index']))
        ES_CLIENT.indices.create(index=job['index'], body=mapping)
    elif does_index_exist and job['wipe']:
        print('{} exists... deleting first'.format(job['index']))
        ES_CLIENT.indices.delete(job['index'])

    csv_generator = csv_chunk_gen(job['file'], chunksize)
    for count, chunk in enumerate(csv_generator):
        print('Running chunk # {}'.format(count))
        iteration = perf_counter()

        if job['wipe'] is False:
            # Future TODO: switch to generated_transaction_unique_id
            id_list = [{'key': c['transaction_id'], 'col':'transaction_id'} for c in chunk]
            delete_transactions_from_es(id_list, job['index'])

        streaming_post_to_es(chunk, job['index'])
        print('Iteration took {}s'.format(perf_counter() - iteration))


def delete_transactions_from_es(id_list, index=None):
    '''
    id_list = [{key:'key1',col:'tranaction_id'},
               {key:'key2',col:'generated_unique_transaction_id'}],
               ...]
    '''
    start = perf_counter()
    print('---------------------------------------------------------------')
    print('Deleting up to {} transaction(s)'.format(len(id_list)))

    if index is None:
        index = '{}-*'.format(settings.TRANSACTIONS_INDEX_ROOT)
    col_to_items_dict = defaultdict(list)
    for id_ in id_list:
        col_to_items_dict[id_['col']].append(id_['key'])

    for column, values in col_to_items_dict.items():
        body = {
            "query": {
                "bool": {
                    "filter": {
                        "terms": {column: values}
                    }
                }
            }
        }
        ES_CLIENT.delete_by_query(index=index, body=body)
    print('ES Deletes took {}s'.format(perf_counter() - start))
