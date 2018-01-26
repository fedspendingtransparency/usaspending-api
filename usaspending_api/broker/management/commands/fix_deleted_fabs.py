"""
Deletes FABS rows that have been deactivated in the broker

Before running, must set up foreign data broker by running
usaspending_api/database_scripts/broker_matviews/broker_server.sql
through psql, after editing to add conneciton specifics
"""
import logging

from django.core.management.base import BaseCommand
from django.db import connections, transaction
from django.db.utils import ProgrammingError

from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.common.helpers import timer

logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")


class Command(BaseCommand):
    help = "Deletes FABS rows that have been deactivated in the broker"

    def add_arguments(self, parser):
        parser.add_argument('-b', '--batchsize', type=int, default=1000, help='Fetch rows for deletion in batches of N')
        parser.add_argument('--batches', type=int, default=0, help='Stop after N batches (deletes all by default)')

    @staticmethod
    def fabs_cursor(limit=None):
        db_cursor = connections['default'].cursor()
        db_query = """
            WITH deletable AS (
                SELECT  afa_generated_unique,
                        bool_or(is_active) AS is_active,
                        bool_or(correction_late_delete_ind in ('d', 'D'))
                            AS deleted
                FROM    broker.published_award_financial_assistance
                GROUP BY 1
                HAVING  bool_or(is_active) = false
                AND     bool_or(correction_late_delete_ind in ('d', 'D'))
                )
            SELECT tf.afa_generated_unique
            FROM   transaction_fabs tf
            JOIN   deletable d USING (afa_generated_unique)
            """

        if limit:
            db_query += ' LIMIT {}'.format(limit)
        try:
            db_cursor.execute(db_query)
        except ProgrammingError as e:
            if 'broker.published_award_financial_assistance' in str(e):
                msg = str(e) + '\nRun database_scripts/broker_matviews/broker_server.sql\n\n'
                raise ProgrammingError(str(e) + msg)
            else:
                raise

        return db_cursor

    @transaction.atomic
    def handle(self, *args, **options):
        logger.info('Starting row deletion...')

        if options['batches']:
            limit = options['batches'] * options['batchsize']
        else:
            limit = None
        with timer('executing query', logger.info):
            cursor = self.fabs_cursor(limit)
        batch_no = 1
        while ((not options['batches']) or (batch_no <= options['batches'])):
            message = 'Batch {} of {} rows'.format(batch_no, options['batchsize'])
            with timer(message, logging.info):
                rows = cursor.fetchmany(options['batchsize'])
            if not rows:
                logger.info('No further rows; finished')
                return
            ids = [r[0] for r in rows]
            with timer('deleting rows', logger.info):
                TransactionNormalized.objects.\
                    filter(assistance_data__afa_generated_unique__in=ids).delete()
            batch_no += 1
        logger.info('{} batches finished, complete'.format(batch_no - 1))
