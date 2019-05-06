import logging
import psycopg2

from psycopg2.sql import Literal, SQL

from usaspending_api.awards.models import TransactionDelta

logger = logging.getLogger('console')


def clean_out_transaction_deltas(connection_string):
    """
    Delete transaction_delta records from the database pointed to by connection_string
    that are <= the max created_at date in the transaction_delta table in the current
    database.
    """
    # Get the max create_at from the transaction_delta in the current database.
    max_created_at = TransactionDelta.objects.get_max_created_at()
    if max_created_at:

        logger.info('Removing transaction_deltas <= {}'.format(max_created_at))

        # We are deleting things from an entirely different database so we'll just
        # use straight up psycopg SQL queries.
        with psycopg2.connect(connection_string) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(
                    SQL('delete from transaction_delta where created_at <= {}').format(Literal(max_created_at)))

    else:
        logger.info('Nothing to remove from transaction_delta')


def ping_transaction_delta(connection_string):
    """
    This is just a sanity check to ensure we can connect with the provided
    connection string and it has the table we'll need.
    """
    with psycopg2.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute('select 1 from transaction_delta where 0 = 1')
