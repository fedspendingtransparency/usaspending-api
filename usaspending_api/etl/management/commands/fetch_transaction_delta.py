import boto3
import csv
import datetime
import os
import pandas as pd
import pytz
import subprocess
import tempfile

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection
from time import perf_counter


TEMP_ES_DELTA_VIEW = '''
CREATE OR REPLACE VIEW transaction_delta_view AS
SELECT
  UTM.awarding_agency_id,
  UTM.transaction_id,
  TM.modification_number,
  UAM.award_id,
  AW.description AS award_description,
  UTM.piid,
  UTM.fain,
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

  UTM.pop_country_name,
  UTM.pop_country_code,
  UTM.pop_zip5,
  UTM.pop_county_code,
  UTM.pop_county_name,
  UTM.pop_state_code,
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
COPY_SQL = '''
"COPY (
    SELECT *
    FROM transaction_delta_view
) TO STDOUT DELIMITER ',' CSV HEADER" > '{dir}{fy}_transactions_{now}.csv'
'''

CHECK_IDS_SQL = '''
WITH all_values AS (SELECT * FROM (VALUES {}) as all_ids (detached_award_proc_unique))
select detached_award_proc_unique, update_date from transaction_delta_view
where EXISTS (SELECT * FROM all_values WHERE transaction_delta_view.detached_award_proc_unique = all_values.detached_award_proc_unique);
'''

HERE = os.path.dirname(os.path.abspath(__file__)) + os.sep
TEMP_SQL_FILE = os.path.join(HERE, 'temp_sql_cmds.sql')


class Command(BaseCommand):
    help = 'Creates a CSV dump of transactions'

    def add_arguments(self, parser):
        parser.add_argument('--fiscal_year', type=int, required=True)
        parser.add_argument('--diff_from', default='2008-10-01', type=str)
        parser.add_argument('--verbose', action='store_true')

    def create_sql_commands(self):
        update_date_str = UPDATE_DATE_SQL.format(self.diff_from.strftime('%Y-%m-%d'))
        view_sql = TEMP_ES_DELTA_VIEW.format(fy=self.fiscal_year, update_date=update_date_str)
        copy_sql = COPY_SQL.format(dir=HERE, fy=self.fiscal_year, now=self.formatted_now)

        if self.deleted_ids:
            id_list = ','.join(["('{}')".format(x) for x in self.deleted_ids.keys()])
            id_sql = CHECK_IDS_SQL.format(id_list)
        else:
            id_sql = None

        return view_sql, copy_sql, id_sql

    def run_db_commands(self):
        start = perf_counter()
        view_sql, copy_sql, id_sql = self.create_sql_commands()

        self.execute_sql(view_sql)
        # It is prefereable to not use shell=True, but this command works. Limited user-input so risk is low
        subprocess.Popen('psql "${{DATABASE_URL}}" -c {}'.format(copy_sql), shell=True).wait()

        if id_sql:
            restored_ids = self.execute_sql(id_sql, True)
            print(restored_ids)
            print('{} "deleted" IDs were found'.format(len(restored_ids)))
            self.correct_deleted_ids()

        self.execute_sql('DROP VIEW transaction_delta_view;')
        print("Database commands took {} seconds".format(perf_counter() - start))

    def dictfetchall(self, cursor):
        "Return all rows from a cursor as a dict"
        columns = [col[0] for col in cursor.description]
        return [
            dict(zip(columns, row))
            for row in cursor.fetchall()
        ]

    def execute_sql(self, cmd, results=False):
        rows = None
        if self.verbose:
            print(cmd)
        with connection.cursor() as cursor:
            cursor.execute(cmd)
            if results:
                rows = self.dictfetchall(cursor)
        return rows

    def gather_deleted_ids(self):
        start = perf_counter()
        s3 = boto3.resource('s3', region_name=settings.CSV_AWS_REGION)
        bucket = s3.Bucket(settings.CSV_2_S3_BUCKET_NAME)
        bucket.objects.all()

        if self.verbose:
            print('=======================')
            print('From {} to now...'.format(self.diff_from))

        to_datetime = datetime.datetime.combine(self.diff_from, datetime.datetime.min.time(), tzinfo=pytz.UTC)
        csv_list = [
            x for x in bucket.objects.all()
            if x.key.endswith('.csv') and x.last_modified >= to_datetime]

        if self.verbose:
            print('Found {} csv files'.format(len(csv_list)))

        for obj in csv_list:
            # Use temporary files to facilitate the csv files from S3 into pands
            (file, file_path) = tempfile.mkstemp()
            bucket.download_file(obj.key, file_path)
            # Ingests the CSV into a dataframe. pandas thinks some ids are dates, so disable parsing
            data = pd.DataFrame.from_csv(file_path, parse_dates=False)
            new_ids = list(data['detached_award_proc_unique'].values)
            # Next statements are ugly, but properly handle the temp files
            os.close(file)
            os.remove(file_path)
            if self.verbose:
                print('file {} has {} ids'.format(obj.key, len(new_ids)))

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
        for record in existing_ids:
            uid = record['detached_award_proc_unique']
            if uid in self.deleted_ids:
                if record['update_date'] > self.deleted_ids[uid]['timestamp']:
                    del self.deleted_ids[uid]

    def handle(self, *args, **options):
        start = perf_counter()
        self.deleted_ids = {}
        self.formatted_now = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')  # ISO8601
        self.verbose = options['verbose']
        self.fiscal_year = options['fiscal_year']

        try:
            self.diff_from = datetime.date(*[int(x) for x in options['diff_from'].split('-')])
        except Exception:
            print('Malformed date string provided for `diff_from`. Requires YYYY-MM-DD')
            raise SystemExit

        # 1. Gather the list of deleted transactions
        # 2. Create temp view
        # 3. Copy/dump transaction rows to CSV file
        # 4. Compare row IDs to see if a "deleted" transaction is back and remove from list
        # 5. Store deleted ids in separate file
        # 6. ?? Send files to S3 ??

        self.gather_deleted_ids()
        self.run_db_commands()

        csv_file = HERE + 'deleted_ids_{}.csv'.format(self.formatted_now)

        with open(csv_file, 'w') as f:
            f.writelines('\n'.join(self.deleted_ids.keys()))

        print("Script completed in {} seconds".format(perf_counter() - start))
