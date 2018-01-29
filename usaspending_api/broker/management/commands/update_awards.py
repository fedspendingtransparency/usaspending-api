import logging

from django.core.management.base import BaseCommand
from django.db import transaction

from usaspending_api.awards.models import TransactionNormalized, TransactionFPDS
from usaspending_api.common.helper import timer
from usaspending_api.etl.award_helpers import update_awards, update_contract_awards, update_award_categories


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

        award_update_id_list = []
        award_contract_update_id_list = []

        if not all_records_flag:
            if fiscal_year:
                fiscal_year = fiscal_year[0]
                logger.info('Processing data for Fiscal Year ' + str(fiscal_year))
            else:
                fiscal_year = 2017

            # Lists to store for update_awards and update_contract_awards
            award_update_id_list = TransactionNormalized.objects.filter(action_date__fy=fiscal_year).\
                values_list('award_id', flat=True)
            award_contract_update_id_list = TransactionFPDS.objects.filter(action_date__fy=fiscal_year).\
                values_list('transaction__award_id', flat=True)

        with timer('updating awards to reflect their latest associated transaction info', logger.info):
            update_awards() if all_records_flag else update_awards(tuple(award_update_id_list))

        with timer('updating contract-specific awards to reflect their latest transaction info...', logger.info):
            update_contract_awards() if all_records_flag else update_contract_awards(tuple(award_contract_update_id_list))

        logger.info('updating award category variables', logger.info):
            update_award_categories() if all_records_flag else update_award_categories(tuple(award_update_id_list))

        # Done!
        logger.info('FINISHED')
