from time import perf_counter
from django.core.management.base import BaseCommand
from django.db import connection
from django.conf import settings
from usaspending_api.bulk_download.filestreaming.s3_handler import S3Handler


TEMP_ES_DELTA_VIEW = '''
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

action_date_sql = ' AND TM.update_date >= \'{}\''


class Command(BaseCommand):

    @staticmethod
    def run_sql_file(file_path):
        sql = Command.create_view_sql()
        with connection.cursor() as cursor:
            # with open(file_path) as infile:
            #     for raw_sql in infile.read().split('\n\n\n'):
            #         if raw_sql.strip():

            # 1. Create temp view
            # 2. pump rows to CSV
            # 3. Compare row IDs to see if a "removed" transaction is back
            cursor.execute(sql)

    @staticmethod
    def create_view_sql(fy, update_date=None):
        update_date_str = ''
        if update_date:
            update_date_str = ''
        return TEMP_ES_DELTA_VIEW.format(fy=fy, update_date=update_date_str)

    def gather_deleted_ids(self):
        s3handler = S3Handler(name='fpds-deleted-records', region=settings.BULK_DOWNLOAD_AWS_REGION)
        file_list = [f for f in s3handler.get_file_list() if f.lower().endswith('.csv')]
        print([f for f in file_list])

    def handle(self, *args, **options):
        start = perf_counter()
        self.gather_deleted_ids()

        print('Finished script in {} seconds'.format(perf_counter() - start))
