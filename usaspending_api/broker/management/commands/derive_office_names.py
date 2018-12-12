import logging
import signal

from django.db import connection, transaction

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.management import load_base

logger = logging.getLogger('console')


class Command(load_base.Command):
    """
    This command will derive all FABS office names from the office codes in the Office table in the DATA Act broker. It
    will create a temporary Office table to JOIN on.
    """

    help = "Derives all FABS office names from the office codes in the Office table in the DATA Act broker. The \
                DATA_BROKER_DATABASE_URL environment variable must set so we can pull Office data from their db."

    @transaction.atomic
    def handle_loading(self, db_cursor, *args, **options):

        def signal_handler(signal, frame):
            transaction.set_rollback(True)
            raise Exception('Received interrupt signal. Aborting...')

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info('Creating a temporary Office table copied from the Broker...')
        db_cursor.execute('SELECT office_name, office_code FROM office')
        all_offices = dictfetchall(db_cursor)
        all_offices_str = ', '.join(["('" + o['office_name'] + "','" + o['office_code'] + "')" for o in all_offices])

        ds_cursor = connection.cursor()
        ds_cursor.execute('CREATE TABLE temp_broker_office (office_name TEXT, office_code TEXT)')
        ds_cursor.execute('INSERT INTO temp_broker_office (office_name, office_code) VALUES ' + all_offices_str)

        logger.info('Deriving FABS awarding_office_names with awarding_office_codes from the temporary Office table...')
        ds_cursor.execute("UPDATE transaction_fabs AS t_fabs "
                          "SET awarding_office_name = office.office_name "
                          "FROM temp_broker_office AS office "
                          "WHERE t_fabs.awarding_office_code = office.office_code "
                          "  AND t_fabs.action_date >= '2018/10/01' "
                          "  AND t_fabs.awarding_office_name IS NULL "
                          "  AND t_fabs.awarding_office_code IS NOT NULL")

        logger.info('Deriving FABS funding_office_names with funding_office_codes from the temporary Office table...')
        ds_cursor.execute("UPDATE transaction_fabs AS t_fabs "
                          "SET funding_office_name = office.office_name "
                          "FROM temp_broker_office AS office "
                          "WHERE t_fabs.funding_office_code = office.office_code "
                          "  AND t_fabs.action_date >= '2018/10/01' "
                          "  AND t_fabs.funding_office_name IS NULL "
                          "  AND t_fabs.funding_office_code IS NOT NULL")

        logger.info('Dropping temporary Office table...')
        ds_cursor.execute('DROP TABLE temp_broker_office')

        logger.info('Finished derivations.')
