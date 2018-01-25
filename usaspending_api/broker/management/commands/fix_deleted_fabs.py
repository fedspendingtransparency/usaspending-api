"""
Deletes FABS rows that have been deactivated in the broker

Before running, must set up foreign data broker by running
usaspending_api/database_scripts/broker_matviews/broker_server.sql
through psql, after editing to add conneciton specifics
"""
import logging
import timeit
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from django.db import connections, transaction

from usaspending_api.awards.models import TransactionFABS, TransactionNormalized, Award
from usaspending_api.broker import lookups
from usaspending_api.etl.management.load_base import load_data_into_model, format_date, get_or_create_location
from usaspending_api.references.models import LegalEntity, Agency, ToptierAgency, SubtierAgency
from usaspending_api.etl.award_helpers import update_awards, update_award_categories

# start = timeit.default_timer()
# function_call
# end = timeit.default_timer()
# time elapsed = str(end - start)


logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

subtier_agency_map = {
    subtier_agency['subtier_code']: subtier_agency['subtier_agency_id']
    for subtier_agency in SubtierAgency.objects.values('subtier_code', 'subtier_agency_id')
    }
subtier_to_agency_map = {
    agency['subtier_agency_id']: {'agency_id': agency['id'], 'toptier_agency_id': agency['toptier_agency_id']}
    for agency in Agency.objects.values('id', 'toptier_agency_id', 'subtier_agency_id')
    }
toptier_agency_map = {
    toptier_agency['toptier_agency_id']: toptier_agency['cgac_code']
    for toptier_agency in ToptierAgency.objects.values('toptier_agency_id', 'cgac_code')
    }

award_update_id_list = []


class Command(BaseCommand):
    help = "Deletes FABS rows that have been deactivated in the broker"

    @staticmethod
    def get_fabs_data():
        import pdb; pdb.set_trace()
        db_cursor = connections['default'].cursor()

        # The ORDER BY is important here because deletions must happen in a specific order and that order is defined
        # by the Broker's PK since every modification is a new row
        db_query = """
            SELECT tf.afa_generated_unique
            FROM   transaction_fabs tf
            JOIN   broker.published_award_financial_assistance pafa
              ON (pafa.afa_generated_unique = tf.afa_generated_unique)
            WHERE  NOT pafa.is_active
            -- ORDER BY tf.published_award_financial_assistance_id ASC
            LIMIT 10"""

        db_cursor.execute(db_query)
        # ids_to_delete = set(r[0] for r in db_cursor.fetchall())

        # logger.info('Number of records to delete: %d' % len(ids_to_delete))

        return db_cursor

    @staticmethod
    def delete_stale_fabs(ids_to_delete=None):
        logger.info('Starting deletion of stale FABS data')

        if ids_to_delete:
            TransactionNormalized.objects.\
                filter(assistance_data__afa_generated_unique__in=ids_to_delete).delete()


    @transaction.atomic
    def handle(self, *args, **options):
        logger.info('Starting row deletion...')

        logger.info('Finding rows to delete...')
        start = timeit.default_timer()
        ids_to_delete = self.get_fabs_data()
        end = timeit.default_timer()
        logger.info('Finished finding rows to delete in ' + str(end - start) + ' seconds')

        total_rows_delete = len(ids_to_delete)

        if total_rows_delete > 0:
            logger.info('Deleting stale FABS data...')
            start = timeit.default_timer()
            self.delete_stale_fabs(ids_to_delete=ids_to_delete)
            end = timeit.default_timer()
            logger.info('Finished deleting stale FABS data in ' + str(end - start) + ' seconds')
        else:
            logger.info('Nothing to delete...')
