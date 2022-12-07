"""
Creates legal entities and adds them to transaction normalized rows and award rows based on transaction_location_data
from `create_locations`
"""

import logging
from datetime import datetime

from django.core.management.base import BaseCommand
from django.db import connection


logger = logging.getLogger("script")
exception_logger = logging.getLogger("exceptions")


class Command(BaseCommand):
    def handle(self, *args, **options):

        with connection.cursor() as curs:
            start = datetime.now()
            logger.info("Running updates on transaction_normalized")
            curs.execute(self.TRANSACTION_NORMALIZED_TFA)
            logger.info(
                "Finished updates on transaction_normalized in %s seconds (%s rows updated)"
                % (str(datetime.now() - start), curs.rowcount)
            )

            start = datetime.now()
            logger.info("Running updates on awards")
            curs.execute(self.AWARDS_TFA)
            logger.info(
                "Finished updates on awards in %s seconds (%s rows updated)"
                % (str(datetime.now() - start), curs.rowcount)
            )

    TRANSACTION_NORMALIZED_TFA = """
        UPDATE transaction_normalized AS txn
        SET non_federal_funding_amount = txf.non_federal_funding_amount,
        funding_amount = COALESCE(txf.federal_action_obligation, 0) + COALESCE(txf.non_federal_funding_amount, 0)
        FROM transaction_fabs AS txf
        WHERE txf.transaction_id = txn.id;
    """

    AWARDS_TFA = """
        UPDATE award_search AS aw
        SET total_funding_amount = (
            SELECT SUM(funding_amount)
            FROM transaction_normalized AS txn
            WHERE txn.award_id = aw.award_id
        )
        WHERE category IS NOT NULL AND category != 'contract';
    """
