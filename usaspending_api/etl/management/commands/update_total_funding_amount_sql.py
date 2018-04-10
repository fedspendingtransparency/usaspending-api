"""
Creates legal entities and adds them to transaction normalized rows and award rows based on transaction_location_data
from `create_locations`
"""

import logging

from django.core.management.base import BaseCommand
from django.db import connection


logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")


class Command(BaseCommand):

    def handle(self, *args, **options):

        with connection.cursor() as curs:
            curs.execute(self.TRANSACTION_FABS_TFA)
            logger.info('Updated FABS non_federal_funding_amount, funding_amount in transaction_normalized. '
                        '{} successful updates'.format(curs.rowcount))

            curs.execute(self.SUM_TRANSACTION_TFA)
            logger.info('Updated FABS total_funding_amount in awards. {} successful updates'.format(curs.rowcount))

    TRANSACTION_FABS_TFA = """
        UPDATE transaction_normalized AS txn
        SET non_federal_funding_amount = txf.non_federal_funding_amount,
        funding_amount = federal_action_obligation + txf.non_federal_funding_amount
        FROM transaction_fabs AS txf
        WHERE txf.transaction_id = txn.id;
    """

    SUM_TRANSACTION_TFA = """
        UPDATE awards AS aw
        SET total_funding_amount = (
            SELECT SUM(funding_amount)
            FROM transaction_normalized AS txn
            WHERE txn.award_id = aw.id
        )
        WHERE category IS NOT NULL AND category != 'contract';
    """
