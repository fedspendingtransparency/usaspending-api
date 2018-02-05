import json
import os
import pandas as pd
import subprocess

from collections import defaultdict
from datetime import date
from datetime import datetime
from django.core.management.base import BaseCommand
from elasticsearch import Elasticsearch
from time import perf_counter

from usaspending_api import settings
from usaspending_api.etl.es_etl_helpers import AWARD_DESC_CATEGORIES
from usaspending_api.etl.es_etl_helpers import configure_sql_strings
from usaspending_api.etl.es_etl_helpers import DataJob
from usaspending_api.etl.es_etl_helpers import execute_sql_statement
from usaspending_api.etl.es_etl_helpers import post_to_elasticsearch
from usaspending_api.etl.es_etl_helpers import VIEW_COLUMNS
from usaspending_api.etl.management.commands.fetch_transactions import gather_deleted_ids
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
        self.config['formatted_now'] = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')  # ISO8601
        self.config['verbose'] = True if options['verbosity'] > 1 else False
        self.config['fiscal_year'] = options['fiscal_year']
        self.config['directory'] = options['dir'] + os.sep
        self.config['provide_deleted'] = options['deleted']
        self.config['recreate'] = options['recreate']

        try:
            self.config['starting_date'] = date(*[int(x) for x in options['since'].split('-')])
        except Exception:
            print('Malformed date string provided. `--since` requires YYYY-MM-DD')
            raise SystemExit

        if self.config['recreate'] and self.config['starting_date'] != date(2001, 1, 1):
            print('Bad mix of parameters! An index should not be dropped if only a subset of data will be loaded')
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

            index = '{}-{}-{}'.format(
                settings.TRANSACTIONS_INDEX_ROOT,
                award_category,
                self.config['fiscal_year'])

            job = DataJob(None, index, self.config['fiscal_year'], awd_cat_idx, filename)
            job.count = self.download_db_records(awd_cat_idx, self.config['fiscal_year'], filename)

            # Upload CSV to ES
            post_to_elasticsearch(ES_CLIENT, job, self.config)

            # Delete File
            os.remove(filename)
            # repeat

        print('Completed all categories for FY{}'.format(self.config['fiscal_year']))

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
        copy_sql, _, count_sql = configure_sql_strings(config, filename, [])

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


def filter_query(column, values, query_type="match_phrase"):
    queries = [{query_type: {column: str(i)}} for i in values]
    body = {
        "query": {
            "bool": {
                "should": [
                    queries
                ]
            }
        }
    }

    return json.dumps(body)


def delete_query(response):
    body = {
        "query": {
            "ids": {
                "type": "transaction_mapping",
                "values": [i['_id'] for i in response['hits']['hits']]
            }
        }
    }
    return json.dumps(body)


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def delete_transactions_from_es(id_list, index=None, size=50000):
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
    start_ = ES_CLIENT.search(index=index)['hits']['total']
    print('Starting amount of indices ----- {}'.format(start_))
    col_to_items_dict = defaultdict(list)
    for l in id_list:
        col_to_items_dict[l['col']].append(l['key'])

    for column, values in col_to_items_dict.items():
        print('Deleting from col {}'.format(column))
        values_generator = chunks(values, 1000)
        for values_ in values_generator:
            body = filter_query(column, values_)
            response = ES_CLIENT.search(index=index, body=body, size=size)
            delete_body = delete_query(response)
            ES_CLIENT.delete_by_query(index=index, body=delete_body, size=size)
    end_ = ES_CLIENT.search(index=index)['hits']['total']
    print('Deleted {} records'.format(str(start_ - end_)))
    print('ES Deletes took {}s'.format(perf_counter() - start))
