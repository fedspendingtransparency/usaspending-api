import logging
import timeit

from django.core.management.base import BaseCommand, CommandError
from django.db import connection

logger = logging.getLogger('console')


class Command(BaseCommand):
    help = "Update Agency codes from broker that are 999 in the website"

    @staticmethod
    def get_broker_data(fiscal_year, fy_start, fy_end):
        sql_statment = """
        CREATE TEMPORARY TABlE fpds_agencies_to_update_{fiscal_year} AS
        SELECT * FROM dblink('broker_server',
        '
        SELECT
            detached_award_procurement_id,
            detached_award_proc_unique,
            awarding_agency_code,
            funding_agency_code
        FROM detached_award_procurement
            WHERE
            action_date::date >= ''{fy_start}''::date
            AND action_date::date <= ''{fy_end}''::date
            ;')
        AS (
            detached_award_procurement_id  text,
            detached_award_proc_unique  text,
            awarding_agency_code  text,
            funding_agency_code  text
            )
       EXCEPT
        SELECT
            detached_award_procurement_id,
            detached_award_proc_unique,
            awarding_agency_code,
            funding_agency_code
        FROM transaction_fpds
        WHERE action_date::date >= '{fy_start}'::date
        AND action_date::date <= '{fy_end}'::date;
        -- Adding Indexes
        CREATE INDEX unique_id_index ON fpds_agencies_to_update_{fiscal_year}(detached_award_proc_unique);
        ANALYZE fpds_agencies_to_update_{fiscal_year};
        """.format(fiscal_year=fiscal_year, fy_start=fy_start, fy_end=fy_end)

        return sql_statment

    @staticmethod
    def update_website(fiscal_year):
        sql_statement = """
        -- Updating awarding agency code
        UPDATE transaction_fpds
        SET
            awarding_agency_code = broker.awarding_agency_code
        FROM
            fpds_agencies_to_update_{fiscal_year} broker
        WHERE
            transaction_fpds.detached_award_proc_unique = broker.detached_award_proc_unique
            AND
            transaction_fpds.awarding_agency_code = '999';
        -- Updating funding agency code
        UPDATE transaction_fpds
        SET
            funding_agency_code = broker.funding_agency_code
        FROM
            fpds_agencies_to_update_{fiscal_year} broker
        WHERE
            transaction_fpds.detached_award_proc_unique = broker.detached_award_proc_unique
            AND
            transaction_fpds.funding_agency_code = '999';
        """.format(fiscal_year=fiscal_year)

        return sql_statement

    def add_arguments(self, parser):
        parser.add_argument(
            '--fiscal_year',
            type=int,
            help='Fiscal year to chose to pull from Broker'
        )

    def handle(self, *args, **options):
        """
        Updates the agency codes in the website transaction tables from broker where code is 999
        """
        fiscal_year = options.get('fiscal_year')

        if not fiscal_year:
            raise CommandError('Must specify --fiscal_year')

        logger.info('Starting script to update agency codes in FPDS from broker')

        db_cursor = connection.cursor()

        fy_start = '10/01/' + str(fiscal_year - 1)
        fy_end = '09/30/' + str(fiscal_year)

        logger.info('Retrieving rows to update from broker')
        start = timeit.default_timer()

        # Comparing broker rows with website for a specific fiscal year
        db_cursor.execute(self.get_broker_data(fiscal_year, fy_start, fy_end))

        end = timeit.default_timer()
        logger.info('Finished retrieving {} data from broker to update in website in {}s'.format(
            fiscal_year, end-start))

        logger.info('Updating transaction_fpds rows with agency codes 999')
        start = timeit.default_timer()

        # Updates website rows with agency code 999
        db_cursor.execute(self.update_website(fiscal_year))

        end = timeit.default_timer()
        logger.info(
            'Finished updating {} transaction fpds rows in {}s'.format(
                fiscal_year, end - start))

        logger.info('Updating transaction fpds agency codes for {}'.format(fiscal_year))
