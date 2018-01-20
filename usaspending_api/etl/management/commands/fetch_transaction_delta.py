from time import perf_counter
from django.core.management.base import BaseCommand
from django.db import connection
import datetime
import pandas as pd
import tempfile
from django.conf import settings
import boto3
import os
import pytz

TEMP_ES_DELTA_VIEW = '''
CREATE TEMPORARY VIEW transaction_delta_view AS
SELECT
  UTM.awarding_agency_id,
  UTM.transaction_id,
  TM.modification_number,
  UAM.award_id,
  AW.description AS award_description,
  UTM.piid,
  UTM.fain,
  UTM.psc_code AS product_or_service_code,
  UTM.psc_description AS product_or_service_description,
  UTM.naics_code,
  UTM.naics_description,
  UAM.type_description,
  UTM.award_category,
  UTM.recipient_unique_id AS duns,
  UTM.parent_recipient_unique_id AS parent_duns,
  UTM.recipient_name,
  UTM.funding_toptier_agency_name,
  UTM.funding_subtier_agency_name,
  UTM.action_date,
  UAM.period_of_performance_start_date,
  UAM.period_of_performance_current_end_date,
  UTM.fiscal_year AS transaction_fiscal_year,
  UAM.fiscal_year AS award_fiscal_year,
  UAM.total_obligation AS award_amount,
  UTM.federal_action_obligation AS transaction_amount,
  UAM.face_value_loan_guarantee,
  UAM.original_loan_subsidy_cost,
  UAM.awarding_toptier_agency_name,
  UTM.awarding_subtier_agency_name,
  UTM.awarding_toptier_agency_abbreviation,
  UTM.funding_toptier_agency_abbreviation,
  UTM.awarding_subtier_agency_abbreviation,
  UTM.funding_subtier_agency_abbreviation,
  UTM.cfda_title,
  UTM.cfda_popular_name,
  UTM.pop_country_code,
  UTM.pop_zip5,
  UTM.pop_county_code,
  UTM.pop_county_name,
  UTM.pop_state_code,
  UTM.pop_congressional_code,
  UTM.recipient_location_country_code,
  UTM.recipient_location_zip5,
  UTM.recipient_location_state_code,
  UTM.recipient_location_county_code,
  UTM.pulled_from,
  UTM.type,
  UTM.funding_subtier_agency_name,
  UTM.funding_toptier_agency_name,
  UTM.recipient_location_country_name,
  UTM.pop_country_name,
  UTM.type_of_contract_pricing,
  UTM.type_set_aside,
  UTM.extent_competed,
  TM.update_date

FROM universal_transaction_matview UTM
JOIN transaction_normalized TM ON (UTM.transaction_id = TM.id)
LEFT JOIN transaction_fpds FPDS ON (UTM.transaction_id = FPDS.transaction_id)
LEFT JOIN universal_award_matview UAM ON (UTM.award_id = UAM.award_id)
JOIN awards AW ON (UAM.award_id = AW.id)
WHERE
  UTM.fiscal_year={fy}{update_date};
'''

UPDATE_DATE_SQL = ' AND TM.update_date >= \'{}\''


class Command(BaseCommand):
    help = 'Creates a CSV dump of transactions'

    def add_arguments(self, parser):
        parser.add_argument('--fiscal_year', nargs='+', type=int)
        parser.add_argument('--diff_from', default=None, type=str)
        parser.add_argument('--verbose', action='store_true')

    @staticmethod
    def run_sql_file(file_path):
        view_sql = Command.create_view_sql()
        copy_sql = ''
        with connection.cursor() as cursor:

            cursor.execute(view_sql)

    def create_view_sql(self):
        update_date_str = ''
        if self.diff_from:
            update_date_str = UPDATE_DATE_SQL.format(self.diff_from)
        return TEMP_ES_DELTA_VIEW.format(fy=self.fiscal_year, update_date=update_date_str)

    def gather_deleted_ids(self):
        s3 = boto3.resource('s3', region_name=settings.CSV_AWS_REGION)
        bucket = s3.Bucket(settings.CSV_2_S3_BUCKET_NAME)
        bucket.objects.all()
        # start_date = datetime.datetime(2018, 1, 16, tzinfo=datetime.timezone.utc)
        if self.diff_from:
            print('=======================')
            print('From {} to now...'.format(self.diff_from))

            csv_list = [
                x for x in bucket.objects.all()
                if x.key.endswith('.csv') and x.last_modified >= self.diff_from]
        else:
            csv_list = [
                x for x in bucket.objects.all()
                if x.key.endswith('.csv')]

        if self.verbose:
            print('Found {} csv files'.format(len(csv_list)))
        deleted_ids = {}

        for obj in csv_list:
            # Use temporary files to facilitate the csv files from S3 into pands
            (file, file_path) = tempfile.mkstemp()
            bucket.download_file(obj.key, file_path)
            data = pd.DataFrame.from_csv(file_path, parse_dates=False)
            new_ids = list(data['detached_award_proc_unique'].values)
            # Next statements are ugly, but properly handle the temp files
            os.close(file)
            os.remove(file_path)
            if self.verbose:
                print('file {} has {} ids'.format(obj.key, len(new_ids)))

            for uid in new_ids:
                if uid in deleted_ids:
                    if deleted_ids[uid]['timestamp'] < obj.last_modified:
                        deleted_ids[uid]['timestamp'] = obj.last_modified
                else:
                    deleted_ids[uid] = {'timestamp': obj.last_modified}

        if self.verbose:
            for k, v in deleted_ids.items():
                print('id: {} last modified: {}'.format(k, str(v['timestamp'])))

    def handle(self, *args, **options):
        start = perf_counter()
        if 'fiscal_year' in options:
            self.fiscal_year = options['fiscal_year']
        else:
            print('Need to have a fiscal year parameter')
            raise SystemExit
        self.diff_from = None
        if options['diff_from']:
            # self.diff_from = datetime.date(options['diff_from'].split('-')).isoformat()
            self.diff_from = datetime.datetime.strptime(options['diff_from'], '%Y-%m-%d')
            self.diff_from = self.diff_from.replace(tzinfo=pytz.UTC)
        self.verbose = options['verbose']

        # 1. Gather the list of deleted transactions
        # 2. Create temp view
        # 3. pump rows to CSV
        # 4. Compare row IDs to see if a "deleted" transaction is back
        self.gather_deleted_ids()

        print('Finished script in {} seconds'.format(perf_counter() - start))
