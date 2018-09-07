from datetime import datetime
from django.core.management.base import BaseCommand
from django.db import connection
import logging

logger = logging.getLogger('console')


class Command(BaseCommand):

    @staticmethod
    def run_sql_file(file_path):
        with connection.cursor() as cursor:
            with open(file_path) as infile:
                for raw_sql in infile.read().split('\n\n\n'):
                    if raw_sql.strip():
                        cursor.execute(raw_sql)

    def handle(self, *args, **options):
        bulk_load_file_path = 'usaspending_api/broker/management/sql/'

        # this is listed out to maintain a very specific order
        file_names = ['create_business_categories_functions.sql', 'create_agency_lookup_matview.sql',
                      'create_award_category_table.sql', 'load_fpds.sql', 'load_fabs.sql', 'load_locations.sql',
                      'load_recipients.sql', 'load_transaction_normalized.sql', 'update_transaction_ids.sql',
                      'load_awards.sql', 'update_award_ids.sql', 'restock_exec_comp.sql', 'update_tables.sql',
                      'load_constraints.sql']

        file_names = [bulk_load_file_path + name for name in file_names]

        # matview_file_path = 'usaspending_api/database_scripts/matviews/'
        # file_names += [matview_file_path + f for f in listdir(matview_file_path) if isfile(join(matview_file_path,f))]

        total_start = datetime.now()
        for file_name in file_names:
            start = datetime.now()
            logger.info('Running %s' % file_name)
            self.run_sql_file(file_name)
            logger.info('Finished %s in %s seconds' % (file_name, str(datetime.now()-start)))

        logger.info('Finished all queries in %s seconds' % str(datetime.now()-total_start))
