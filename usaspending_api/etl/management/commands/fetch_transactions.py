import os
import subprocess

from datetime import date
from datetime import datetime
from django.core.management.base import BaseCommand
from time import perf_counter

from usaspending_api.etl.es_etl_helpers import configure_sql_strings
from usaspending_api.etl.es_etl_helpers import execute_sql_statement
from usaspending_api.etl.es_etl_helpers import gather_deleted_ids

# SCRIPT OBJECTIVES and ORDER OF EXECUTION STEPS
# 1. [conditional] Gather the list of deleted transactions from S3
# 2. Create temporary view of transaction records
# 3. Copy/dump transaction rows to CSV file
# 4. [conditional] Compare row IDs to see if a "deleted" transaction is back and remove from list
# 5. [conditional] Store deleted ids in separate file
#    (possible future step) Transfer output csv files to S3


class Command(BaseCommand):
    help = '''
    This script is to gather a single fiscal year of transaction records as a CSV file

    (example: 2018_transactions_20180121T195109Z.csv).

    If the option parameter `--award-type` is provided then the transactions CSV will only have
    transactions of that award type.

    If the optional parameter `-d` is used, a second CSV file will be generated of deleted transactions
    (example: deleted_ids_20180121T200832Z.csv).

    To customize a delta of added/modified transactions from a specific date to present datetime use `--since`
    '''

    # used by parent class
    def add_arguments(self, parser):
        parser.add_argument(
            'fiscal_year',
            type=int,
            help='Desired fiscal year of retrieved transactions')
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
            help='Flag to include deleted transactions into script results')
        parser.add_argument(
            '--award-type',
            choices=['contract', 'grant', 'loans', 'direct payment', 'other'],
            type=str,
            default=None,
            help='If this flag is used the CSV will only have transactions of this award category description')

    # used by parent class
    def handle(self, *args, **options):
        ''' Script execution of custom code starts in this method'''
        print('---------------------------------------------------------------')
        print('NOTICE: This is a legacy script which is no longer used')
        print('        please use es_loader or es_rapidloader')
        print('---------------------------------------------------------------')
        start = perf_counter()

        self.config = set_config()
        self.config['formatted_now'] = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')  # ISO8601
        self.config['verbose'] = True if options['verbosity'] > 1 else False
        self.config['fiscal_year'] = options['fiscal_year']
        self.config['directory'] = options['dir'] + os.sep
        self.config['provide_deleted'] = options['deleted']
        self.config['award_category'] = options['award_type']

        try:
            self.config['starting_date'] = date(*[int(x) for x in options['since'].split('-')])
        except Exception:
            print('Malformed date string provided. `--since` requires YYYY-MM-DD')
            raise SystemExit

        if not os.path.isdir(self.config['directory']):
            print('Provided directory does not exist')
            raise SystemExit

        self.deleted_ids = gather_deleted_ids(self.config)
        self.db_interactions()
        self.write_deleted_ids()

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
        copy_sql, id_sql, count_sql = configure_sql_strings(self.config, filename, self.deleted_ids)

        count = execute_sql_statement(count_sql, True, self.config['verbose'])
        print('\nWriting {} transactions to this file:'.format(count[0]['count']))
        print(filename)
        # It is preferable to not use shell=True, but this command works. Limited user-input so risk is low
        subprocess.Popen('psql "${{DATABASE_URL}}" -c {}'.format(copy_sql), shell=True).wait()

        if id_sql:
            print('Getting the list of "deleted" transactions')
            restored_ids = execute_sql_statement(id_sql, True, self.config['verbose'])
            if self.config['verbose']:
                print(restored_ids)
            print('{} "deleted" IDs were found in DB'.format(len(restored_ids)))
            self.correct_deleted_ids(restored_ids)

        print("Database interactions took {} seconds".format(perf_counter() - start))

    def correct_deleted_ids(self, existing_ids):
        '''
            This function removeds transactions from the deleted list.
            Necessary for when a transaction was previously deleted and then recreted
        '''
        print('Verifying deleted transactions')
        count = 0
        for record in existing_ids:
            uid = record['generated_unique_transaction_id']
            if uid in self.deleted_ids:
                if record['update_date'] > self.deleted_ids[uid]['timestamp']:
                    # Remove recreated transaction from the final list
                    del self.deleted_ids[uid]
                    count += 1
        print('Removed {} IDs from list'.format(count))

    def write_deleted_ids(self):
        ''' Write the list of deleted (and not recreated) transactions to disk'''
        print('---------------------------------------------------------------')
        if not self.config['provide_deleted']:
            print('Skipping deleted transactions output')
            return
        print('There are {} transaction records to delete'.format(len(self.deleted_ids)))
        print('Writing deleted transactions csv. Filepath:')
        csv_file = self.config['directory'] + 'deleted_ids_{}.csv'.format(self.config['formatted_now'])
        print(csv_file)

        with open(csv_file, 'w') as f:
            f.write('generated_unique_transaction_id\n')
            f.writelines('\n'.join(self.deleted_ids.keys()))


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
