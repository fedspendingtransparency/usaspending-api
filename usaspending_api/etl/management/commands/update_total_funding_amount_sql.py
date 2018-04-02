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
            logger.info('total_funding_amount recalculated. {} successful updates'.format(curs.rowcount))

            curs.execute(self.TRANSACTION_NORMALIZED_TFA)
            logger.info('total_funding_amount moved to the transaction normalized table. {} successful updates'.format(
                curs.rowcount))

            curs.execute(self.SUM_TRANSACTION_TFA)
            logger.info('New sum for total_funding_amount. {} successful updates'.format(curs.rowcount))

    TRANSACTION_FABS_TFA = """
        update transaction_normalized as tn
        set non_federal_funding_amount=tf.non_federal_funding_amount
        from transaction_fabs as tf
            where tf.transaction_id=tn.id;
    """

    TRANSACTION_NORMALIZED_TFA = """
        update transaction_normalized as tn
        set funding_amount= federal_action_obligation + non_federal_funding_amount;
    """

    SUM_TRANSACTION_TFA = """
        With sum_table as (
        SELECT award_id, SUM(tn.funding_amount) sum_val
        FROM transaction_normalized as tn join awards a on tn.award_id = a.id WHERE a.id = tn.award_id GROUP BY award_id
        )
        update awards as a
        set total_funding_amount=sum_table.sum_val
        from sum_table
        where a.id = sum_table.award_id;
    """
