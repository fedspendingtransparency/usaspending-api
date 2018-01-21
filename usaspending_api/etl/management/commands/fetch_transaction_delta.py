import boto3
import os
import pandas as pd
import pytz
import subprocess
import tempfile

from datetime import date
from datetime import datetime
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection
from time import perf_counter


HERE = os.path.dirname(os.path.abspath(__file__)) + os.sep

# SCRIPT OBJECTIVES and ORDER
# 1. [conditional] Gather the list of deleted transactions
# 2. Create temp view of transaction records
# 3. Copy/dump transaction rows to CSV file
# 4. [conditional] Compare row IDs to see if a "deleted" transaction is back and remove from list
# 5. [conditional] Store deleted ids in separate file
# 6. (possible future step) Transfer output csv files to S3


class Command(BaseCommand):
    help = '''
    This script is to gather a single fiscal year of transaction records as a CSV file

    (example: 2018_transactions_20180121T195109Z.csv).

    If the optional parameter -d is used, a second CSV file will be generated of deleted transactions
    (example: deleted_ids_20180121T200832Z.csv).

    To customize a delta of added/modified transactions since a specific date use --since
    '''

    # inherited from parent class
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
            default=HERE,
            type=str,
            help='Set for a custom location of output files')
        parser.add_argument(
            '-d',
            '--deleted',
            action='store_true',
            help='Flag to include deleted transactions into script results')

    # inherited from parent class
    def handle(self, *args, **options):
        start = perf_counter()
        self.deleted_ids = {}
        self.formatted_now = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')  # ISO8601
        self.verbose = True if options['verbosity'] > 1 else False
        self.fiscal_year = options['fiscal_year']
        self.directory = options['dir'] + os.sep
        self.provide_deleted = options['deleted']

        try:
            self.starting_date = date(*[int(x) for x in options['since'].split('-')])
        except Exception:
            print('Malformed date string provided. `--since` requires YYYY-MM-DD')
            raise SystemExit

        if not os.path.isdir(self.directory):
            print('Provided directory does not exist')
            raise SystemExit

        self.gather_deleted_ids()
        self.db_interactions()
        self.write_deleted_ids()

        print('---------------------------------------------------------------')
        print("Script completed in {} seconds".format(perf_counter() - start))

    def configure_sql_strings(self, filename):
        update_date_str = UPDATE_DATE_SQL.format(self.starting_date.strftime('%Y-%m-%d'))
        view_sql = TEMP_ES_DELTA_VIEW.format(fy=self.fiscal_year, update_date=update_date_str)
        copy_sql = COPY_SQL.format(filename=filename)

        if self.deleted_ids and self.provide_deleted:
            id_list = ','.join(["('{}')".format(x) for x in self.deleted_ids.keys()])
            id_sql = CHECK_IDS_SQL.format(id_list)
        else:
            id_sql = None

        return view_sql, copy_sql, id_sql

    def db_interactions(self):
        print('---------------------------------------------------------------')
        print('Executing Postgres statements')
        start = perf_counter()
        filename = '{dir}{fy}_transactions_{now}.csv'.format(
            dir=self.directory,
            fy=self.fiscal_year,
            now=self.formatted_now)
        view_sql, copy_sql, id_sql = self.configure_sql_strings(filename)

        execute_sql_statement(view_sql, False, self.verbose)

        print('storing FY transactions here:')
        print(filename)
        # It is preferable to not use shell=True, but this command works. Limited user-input so risk is low
        subprocess.Popen('psql "${{DATABASE_URL}}" -c {}'.format(copy_sql), shell=True).wait()

        if id_sql:
            restored_ids = execute_sql_statement(id_sql, True, self.verbose)
            if self.verbose:
                print(restored_ids)
            print('{} "deleted" IDs were found'.format(len(restored_ids)))
            self.correct_deleted_ids(restored_ids)

        execute_sql_statement('DROP VIEW transaction_delta_view;', False, self.verbose)
        print("Database interactions took {} seconds".format(perf_counter() - start))

    def gather_deleted_ids(self):
        print('---------------------------------------------------------------')
        if not self.provide_deleted:
            print('Skipping the S3 CSV fetch for deleted transactions')
            return
        print('Gathering all deleted transactions from S3')
        start = perf_counter()
        s3 = boto3.resource('s3', region_name=settings.CSV_AWS_REGION)
        bucket = s3.Bucket(settings.CSV_2_S3_BUCKET_NAME)
        bucket.objects.all()

        if self.verbose:
            print('Gathering data from {} to now.'.format(self.starting_date))

        to_datetime = datetime.combine(self.starting_date, datetime.min.time(), tzinfo=pytz.UTC)
        csv_list = [
            x for x in bucket.objects.all()
            if x.key.endswith('.csv') and x.last_modified >= to_datetime]

        if self.verbose:
            print('Found {} csv files'.format(len(csv_list)))

        for obj in csv_list:
            # Use temporary files to facilitate date moving from csv files on S3 into pands
            (file, file_path) = tempfile.mkstemp()
            bucket.download_file(obj.key, file_path)

            # Ingests the CSV into a dataframe. pandas thinks some ids are dates, so disable parsing
            data = pd.DataFrame.from_csv(file_path, parse_dates=False)
            new_ids = list(data['detached_award_proc_unique'].values)

            # Next statements are ugly, but properly handle the temp files
            os.close(file)
            os.remove(file_path)

            if self.verbose:
                print('{:<55} ... {}'.format(obj.key, len(new_ids)))

            for uid in new_ids:
                if uid in self.deleted_ids:
                    if self.deleted_ids[uid]['timestamp'] < obj.last_modified:
                        self.deleted_ids[uid]['timestamp'] = obj.last_modified
                else:
                    self.deleted_ids[uid] = {'timestamp': obj.last_modified}

        if self.verbose:
            for k, v in self.deleted_ids.items():
                print('id: {} last modified: {}'.format(k, str(v['timestamp'])))

        print('Found {} IDs'.format(len(self.deleted_ids)))
        print("Gathering deleted transactions took {} seconds".format(perf_counter() - start))

    def correct_deleted_ids(self, existing_ids):
        '''
            This function removeds transactions from the deleted list.
            Necessary for when a transaction was previously deleted and then recreted
        '''
        print('Verifying deleted transactions')
        count = 0
        for record in existing_ids:
            uid = record['detached_award_proc_unique']
            if uid in self.deleted_ids:
                if record['update_date'] > self.deleted_ids[uid]['timestamp']:
                    # Remove recreated transaction from the final list
                    del self.deleted_ids[uid]
                    count += 1
        print('Removed {} IDs from list'.format(count))

    def write_deleted_ids(self):
        print('---------------------------------------------------------------')
        if not self.provide_deleted:
            print('Skipping deleted transactions output')
            return
        print('There are {} transaction records to delete'.format(len(self.deleted_ids)))
        print('Writing deleted transactions csv. Filepath:')
        csv_file = self.directory + 'deleted_ids_{}.csv'.format(self.formatted_now)
        print(csv_file)

        with open(csv_file, 'w') as f:
            f.writelines('\n'.join(self.deleted_ids.keys()))


def db_rows_to_dict(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def execute_sql_statement(cmd, results=False, verbose=False):
    rows = None
    if verbose:
        print(cmd)
    with connection.cursor() as cursor:
        cursor.execute(cmd)
        if results:
            rows = db_rows_to_dict(cursor)
    return rows


# ==============================================================================
# SQL Strings for Postgres Statements


TEMP_ES_DELTA_VIEW = '''
CREATE OR REPLACE VIEW transaction_delta_view AS
SELECT
  UTM.transaction_id,
  TM.modification_number,
  UAM.award_id,
  UTM.piid,
  UTM.fain,
  UTM.uri,
  AW.description AS award_description,

  UTM.product_or_service_code,
  UTM.product_or_service_description,
  UTM.naics_code,
  UTM.naics_description,
  UAM.type_description,
  UTM.award_category,
  UTM.recipient_unique_id,
  UTM.parent_recipient_unique_id,
  UTM.recipient_name,

  UTM.action_date,
  UAM.period_of_performance_start_date,
  UAM.period_of_performance_current_end_date,
  UTM.fiscal_year AS transaction_fiscal_year,
  UAM.fiscal_year AS award_fiscal_year,
  UAM.total_obligation,
  UTM.federal_action_obligation,
  UAM.face_value_loan_guarantee,
  UAM.original_loan_subsidy_cost,

  UTM.awarding_agency_id,
  UTM.funding_agency_id,
  UAM.awarding_toptier_agency_name,
  UTM.funding_toptier_agency_name,
  UTM.awarding_subtier_agency_name,
  UTM.funding_subtier_agency_name,
  UTM.awarding_toptier_agency_abbreviation,
  UTM.funding_toptier_agency_abbreviation,
  UTM.awarding_subtier_agency_abbreviation,
  UTM.funding_subtier_agency_abbreviation,

  UTM.cfda_title,
  UTM.cfda_popular_name,
  UTM.type_of_contract_pricing,
  UTM.type_set_aside,
  UTM.extent_competed,
  UTM.pulled_from,
  UTM.type,

  UTM.pop_country_code,
  UTM.pop_country_name,
  UTM.pop_state_code,
  UTM.pop_county_code,
  UTM.pop_county_name,
  UTM.pop_zip5,
  UTM.pop_congressional_code,

  UTM.recipient_location_country_code,
  UTM.recipient_location_country_name,
  UTM.recipient_location_state_code,
  UTM.recipient_location_county_code,
  UTM.recipient_location_zip5,

  FPDS.detached_award_proc_unique,
  TM.update_date

FROM universal_transaction_matview UTM
JOIN transaction_normalized TM ON (UTM.transaction_id = TM.id)
LEFT JOIN transaction_fpds FPDS ON (UTM.transaction_id = FPDS.transaction_id)
LEFT JOIN universal_award_matview UAM ON (UTM.award_id = UAM.award_id)
JOIN awards AW ON (UAM.award_id = AW.id)
WHERE
  UTM.fiscal_year={fy}{update_date};'''

UPDATE_DATE_SQL = ' AND TM.update_date >= \'{}\''

COPY_SQL = '''"COPY (
    SELECT *
    FROM transaction_delta_view
) TO STDOUT DELIMITER ',' CSV HEADER" > '{filename}'
'''

CHECK_IDS_SQL = '''
WITH temp_transaction_ids AS (SELECT * FROM (VALUES {}) AS unique_id_list (detached_award_proc_unique))
SELECT detached_award_proc_unique, update_date FROM transaction_delta_view
WHERE EXISTS (
SELECT *
FROM temp_transaction_ids
WHERE transaction_delta_view.detached_award_proc_unique = temp_transaction_ids.detached_award_proc_unique
);
'''
