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

    @transaction.atomic
    def handle(self, *args, **options):
        logger.info('Starting updates to award data...')

        # with connection.cursor() as cursor:
        #     cursor.execute('DELETE * FROM transaction')

        # Lists to store for update_awards and update_contract_awards
        # AWARD_UPDATE_ID_LIST = []
        # AWARD_CONTRACT_UPDATE_ID_LIST = []

        logger.info('Updating awards to reflect their latest associated transaction info...')
        start = timeit.default_timer()
        update_awards()  # we want this to run on everything
        # update_awards(tuple(AWARD_UPDATE_ID_LIST))
        end = timeit.default_timer()
        logger.info('Finished updating awards in ' + str(end - start) + ' seconds')

        logger.info('Updating contract-specific awards to reflect their latest transaction info...')
        start = timeit.default_timer()
        update_contract_awards()  # we want this to run on everything
        # update_contract_awards(tuple(AWARD_CONTRACT_UPDATE_ID_LIST))
        end = timeit.default_timer()
        logger.info('Finished updating contract specific awards in ' + str(end - start) + ' seconds')

        logger.info('Updating award category variables...')
        start = timeit.default_timer()
        update_award_categories()  # we want this to run on everything
        # update_award_categories(tuple(AWARD_UPDATE_ID_LIST))
        end = timeit.default_timer()
        logger.info('Finished updating award category variables in ' + str(end - start) + ' seconds')

        # Done!
        logger.info('FINISHED')