import boto3
import csv
import json
import os
import pandas as pd
import pytz
import subprocess
import tempfile

from datetime import date
from datetime import datetime
from django.core.management.base import BaseCommand
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from time import perf_counter
from usaspending_api import settings
from usaspending_api.etl.management.commands.fetch_transactions import configure_sql_strings
from usaspending_api.etl.management.commands.fetch_transactions import execute_sql_statement

# SCRIPT OBJECTIVES and ORDER OF EXECUTION STEPS
# 1. [conditional] Remove deleted transactions from ES
#   a. Gather the list of deleted transactions from S3
#   b. Delete transactions from ES
# 2. Create temporary view of transaction records
# 3. Iterate over every fiscal year and award type description
#   a. Download 1 CSV file
#   b. Either delete all transactions from ES or delete target index
#   c. Load CSV contents into ES
#   d. Lather. Rinse. Repeat. until all fiscal years are complete


class Command(BaseCommand):
    help = '''
    '''

    # used by parent class
    def add_arguments(self, parser):
        parser.add_argument(
            '--fiscal_year',
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
        self.deleted_ids = {}

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

        self.gather_deleted_ids()
        self.db_interactions()
        self.delete_transactions_from_es()

        print('---------------------------------------------------------------')
        print("Script completed in {} seconds".format(perf_counter() - start))

    def db_interactions(self):
        '''
        All DB statements/commands/transactions are controlled by this method.
        Some are using the django DB client, others using psql
        '''
        print('---------------------------------------------------------------')
        print('Executing Postgres statements')
        start = perf_counter()
        type_str = ''
        if self.config['award_category']:
            type_str = '{}_'.format(self.config['award_category'])
        filename = '{dir}{fy}_transactions_{type}{now}.csv'.format(
            dir=self.config['directory'],
            fy=self.config['fiscal_year'],
            type=type_str,
            now=self.config['formatted_now'])
        view_sql, copy_sql, id_sql, count_sql, drop_view = configure_sql_strings(self.config, filename, self.deleted_ids)

        execute_sql_statement(view_sql, False, self.config['verbose'])

        count = execute_sql_statement(count_sql, True, self.config['verbose'])
        print('\nWriting {} transactions to this file:'.format(count[0]['count']))
        print(filename)
        # It is preferable to not use shell=True, but this command works. Limited user-input so risk is low
        subprocess.Popen('psql "${{DATABASE_URL}}" -c {}'.format(copy_sql), shell=True).wait()

        if id_sql:
            restored_ids = execute_sql_statement(id_sql, True, self.config['verbose'])
            if self.config['verbose']:
                print(restored_ids)
            print('{} "deleted" IDs were found in DB'.format(len(restored_ids)))
            self.correct_deleted_ids(restored_ids)

        execute_sql_statement(drop_view, False, self.config['verbose'])
        print("Database interactions took {} seconds".format(perf_counter() - start))

    def gather_deleted_ids(self):
        '''
        Connect to S3 and gather all of the transaction ids stored in CSV files
        generated by the broker when transactions are removed from the DB.
        '''
        print('---------------------------------------------------------------')
        if not self.config['provide_deleted']:
            print('Skipping the S3 CSV fetch for deleted transactions')
            return
        print('Gathering all deleted transactions from S3')
        start = perf_counter()
        try:
            s3 = boto3.resource('s3', region_name=self.config['aws_region'])
            bucket = s3.Bucket(self.config['s3_bucket'])
            bucket_objects = list(bucket.objects.all())
        except Exception as e:
            print('\n[ERROR]\n')
            print('Verify settings.CSV_AWS_REGION and settings.DELETED_TRANSACTIONS_S3_BUCKET_NAME are correct')
            print('  or is using env variables: CSV_AWS_REGION and DELETED_TRANSACTIONS_S3_BUCKET_NAME')
            print('\n{}\n'.format(e))
            raise SystemExit

        if self.config['verbose']:
            print('CSV data from {} to now.'.format(self.config['starting_date']))

        to_datetime = datetime.combine(self.config['starting_date'], datetime.min.time(), tzinfo=pytz.UTC)
        filtered_csv_list = [
            x for x in bucket_objects
            if (
                x.key.endswith('.csv') and
                not x.key.startswith('staging') and
                x.last_modified >= to_datetime
            )
        ]

        if self.config['verbose']:
            print('Found {} csv files'.format(len(filtered_csv_list)))

        for obj in filtered_csv_list:
            # Use temporary files to facilitate date moving from csv files on S3 into pands
            (file, file_path) = tempfile.mkstemp()
            bucket.download_file(obj.key, file_path)

            # Ingests the CSV into a dataframe. pandas thinks some ids are dates, so disable parsing
            data = pd.DataFrame.from_csv(file_path, parse_dates=False)

            if 'detached_award_proc_unique' in data:
                new_ids = ['cont_tx_' + x for x in data['detached_award_proc_unique'].values]
            elif 'afa_generated_unique' in data:
                new_ids = ['asst_tx_' + x for x in data['afa_generated_unique'].values]
            else:
                print('  [Missing valid col] in {}'.format(obj.key))

            # Next statements are ugly, but properly handle the temp files
            os.close(file)
            os.remove(file_path)

            if self.config['verbose']:
                print('{:<55} ... {}'.format(obj.key, len(new_ids)))

            for uid in new_ids:
                if uid in self.deleted_ids:
                    if self.deleted_ids[uid]['timestamp'] < obj.last_modified:
                        self.deleted_ids[uid]['timestamp'] = obj.last_modified
                else:
                    self.deleted_ids[uid] = {'timestamp': obj.last_modified}

        if self.config['verbose']:
            for k, v in self.deleted_ids.items():
                print('id: {} last modified: {}'.format(k, str(v['timestamp'])))

        print('Found {} IDs'.format(len(self.deleted_ids)))
        print("Gathering deleted transactions took {} seconds".format(perf_counter() - start))

    def delete_transactions_from_es(self):
        ''' Run POST <index>/_delete_by_query command here with list of ids in query'''
        # self.deleted_ids.keys()))
        pass


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

    config = {
        'aws_region': os.environ.get('CSV_AWS_REGION'),
        's3_bucket': os.environ.get('DELETED_TRANSACTIONS_S3_BUCKET_NAME')
    }

    return config
