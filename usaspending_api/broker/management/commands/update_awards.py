import logging
import timeit

from django.core.management.base import BaseCommand
from django.db import connection, transaction

from usaspending_api.awards.models import TransactionNormalized, TransactionFABS, TransactionFPDS
from usaspending_api.etl.award_helpers import update_awards, update_contract_awards, update_award_categories

# start = timeit.default_timer()
# function_call
# end = timeit.default_timer()
# time elapsed = str(end - start)


logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")


class Command(BaseCommand):
    help = "Updates awards based on transactions in the database or based on Award IDs passed in"

    def add_arguments(self, parser):
        parser.add_argument(
            '--fiscal_year',
            dest="fiscal_year",
            nargs='+',
            type=int,
            help="Year for which to run the historical load"
        )

        parser.add_argument(
            '--all',
            action='store_true',
            dest='all',
            default=False,
            help='Runs the award updates on all records'
        )

    @transaction.atomic
    def handle(self, *args, **options):
        logger.info('Starting updates to award data...')

        all_records_flag = options.get('all')
        fiscal_year = options.get('fiscal_year')

        AWARD_UPDATE_ID_LIST = []
        AWARD_CONTRACT_UPDATE_ID_LIST = []

        if not all_records_flag:
            if fiscal_year:
                fiscal_year = fiscal_year[0]
                logger.info('Processing data for Fiscal Year ' + str(fiscal_year))
            else:
                fiscal_year = 2017

            # Lists to store for update_awards and update_contract_awards
            AWARD_UPDATE_ID_LIST = TransactionNormalized.objects.filter(action_date__fy=fiscal_year).\
                values_list('award_id', flat=True)
            AWARD_CONTRACT_UPDATE_ID_LIST = TransactionFPDS.objects.filter(action_date__fy=fiscal_year).\
                values_list('award_id', flat=True)

        logger.info('Updating awards to reflect their latest associated transaction info...')
        start = timeit.default_timer()
        update_awards() if all_records_flag else update_awards(tuple(AWARD_UPDATE_ID_LIST))
        end = timeit.default_timer()
        logger.info('Finished updating awards in ' + str(end - start) + ' seconds')

        logger.info('Updating contract-specific awards to reflect their latest transaction info...')
        start = timeit.default_timer()
        update_contract_awards() if all_records_flag else update_contract_awards(tuple(AWARD_CONTRACT_UPDATE_ID_LIST))
        end = timeit.default_timer()
        logger.info('Finished updating contract specific awards in ' + str(end - start) + ' seconds')

        logger.info('Updating award category variables...')
        start = timeit.default_timer()
        update_award_categories() if all_records_flag else update_award_categories(tuple(AWARD_UPDATE_ID_LIST))
        end = timeit.default_timer()
        logger.info('Finished updating award category variables in ' + str(end - start) + ' seconds')

        # Done!
        logger.info('FINISHED')