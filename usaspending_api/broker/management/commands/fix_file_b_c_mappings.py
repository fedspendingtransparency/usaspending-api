import logging
import timeit

from django.core.management.base import BaseCommand
from django.db import connection, connections
from django.conf import settings

logger = logging.getLogger('console')

# Website columns need for update from financial_accounts_by_program_activity_object_class
financial_accounts_oc = [
    ('broker_submission_id', 'sub'),
    ('allocation_transfer_agency_id', 'tas'),
    ('agency_id', 'tas'),
    ('beginning_period_of_availability', 'tas'),
    ('ending_period_of_availability', 'tas'),
    ('availability_type_code', 'tas'),
    ('main_account_code', 'tas'),
    ('sub_account_code', 'tas'),
    ('object_class', 'oc'),
    ('program_activity_code', 'pa'),
    ('direct_reimbursable', 'oc'),
    ('ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe', ' ds_file_table')
]

# Website columns need for update from financial_accounts_by_awards (Adds fain, uri, and piid columns)
financial_accounts_awards = [('fain', ' ds_file_table'),
                             ('uri', ' ds_file_table'),
                             ('piid', ' ds_file_table')] + \
                             financial_accounts_oc[:]

# website -> broker mappings with specific mappings different from settings.py mappings
broker_website_cols_mapping = {
    'broker_submission_id': 'db_file_table.submission_id',
    "allocation_transfer_agency_id": "COALESCE(allocation_transfer_agency, '''')",
    'agency_id': 'agency_identifier',
    'object_class': 'object_class',
    'program_activity_code': 'program_activity_code',
    'beginning_period_of_availability': "COALESCE(beginning_period_of_availa, '''')",
    'ending_period_of_availability': "COALESCE(ending_period_of_availabil, '''')",
    'availability_type_code': "COALESCE(availability_type_code, '''')",
    'direct_reimbursable': "CASE WHEN by_direct_reimbursable_fun = ''D'' then 1" +
    "WHEN by_direct_reimbursable_fun = ''R'' then 2 ELSE NULL END"
    }


class Command(BaseCommand):
    help = "Fix File B and File C mappings due to invalid database column mappings"

    @staticmethod
    def get_list_of_submissions():
        # Gets a list from broker of the submissions that are certified (type 2) and not updated
        broker_connection = connections['data_broker'].cursor()
        broker_connection.execute('SELECT submission_id from submission sub ' +
                                  'WHERE sub.publish_status_id = 2 ORDER BY submission_id;')
        return broker_connection.fetchall()

    @staticmethod
    def get_list_of_broker_cols(website_cols):
        # Returns sql string statement to pull columns from broker
        # First looks at broker_website_cols_translation that have a specific website to broker mapping
        # Then looks at settings.py for the overall website website->broker mapping
        return ",".join([
            broker_website_cols_mapping.get(column, settings.LONG_TO_TERSE_LABELS.get(column))
            for column, table in website_cols])

    @staticmethod
    def get_list_of_broker_cols_types(website_cols):
        # Returns sql string statement to create table to compare to website type
        # Needs to add a type (text or numeric) in order to match it to the website
        return ",".join([
            "{column} {type}".format(column=column, type='text' if column not in [
                'ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe',
                'broker_submission_id'] else "numeric")
            for column, table in website_cols
        ])

    @staticmethod
    def get_website_row_formatted(website_cols):
        # Return sql string with website columns formatted for select statment
        return ",".join(['{0}.{1}'.format(table, column)
                         for column, table in website_cols])

    @staticmethod
    def get_rows_to_update(file_type, submission_id, broker_cols, broker_cols_type, website_cols):
        # Creates table with rows necessary to update

        query_arguments = {'submission_id': submission_id,
                           'broker_cols': broker_cols,
                           'dblink_cols_type': broker_cols_type,
                           'website_cols': website_cols
                           }

        if file_type == 'B':
            query_arguments['broker_table'] = 'award_financial'
            query_arguments['website_table'] = 'financial_accounts_by_awards'
            query_arguments['broker_tmp_table'] = 'file_b_rows_to_update'
        else:
            query_arguments['broker_table'] = 'object_class_program_activity'
            query_arguments['website_table'] = 'financial_accounts_by_program_activity_object_class'
            query_arguments['broker_tmp_table'] = 'file_c_rows_to_update'

        sql_statement = """
        CREATE TEMPORARY TABLE {broker_tmp_table} AS
        SELECT * from dblink(
            'broker_server',
            'SELECT
                {broker_cols}
                FROM {broker_table} db_file_table
                INNER JOIN  submission sub
                ON db_file_table.submission_id = sub.submission_id
                WHERE sub.publish_status_id = 2
                AND db_file_table.submission_id = {submission_id};'
        ) AS (
            {dblink_cols_type}
        )
        EXCEPT
        SELECT
            {website_cols}
            from {website_table}  ds_file_table
            LEFT OUTER JOIN object_class oc
            ON ds_file_table.object_class_id = oc.id
            LEFT OUTER JOIN ref_program_activity pa
            ON ds_file_table.program_activity_id = pa.id
            LEFT OUTER JOIN treasury_appropriation_account tas
            ON ds_file_table.treasury_account_id = tas.treasury_account_identifier
            INNER JOIN submission_attributes sub
            ON ds_file_table.submission_id = sub.submission_id
            where sub.broker_submission_id = {submission_id};
        """.format(**query_arguments)
        return sql_statement

    @staticmethod
    def update_website_rows(ds_file_table, broker_tmp_table, ds_cols):

        mappings = " AND ".join(['broker.{0} =  {1}.{0}'.format(column, table)
                                 for column, table in ds_cols[:-1]])

        sql_statement = """
        UPDATE {ds_file_table} ds_file_table
        SET  ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe =
            broker.ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe * -1
        FROM
             {broker_tmp_table} broker,
             object_class oc,
             ref_program_activity pa,
             treasury_appropriation_account tas,
             submission_attributes sub
        where  ds_file_table.object_class_id = oc.id
        AND  ds_file_table.program_activity_id = pa.id
        AND  ds_file_table.treasury_account_id = tas.treasury_account_identifier
        AND  ds_file_table.submission_id = sub.submission_id
        AND {table_mappings}
        ;
        """.format(ds_file_table=ds_file_table, broker_tmp_table=broker_tmp_table, table_mappings=mappings)

        return sql_statement

    def handle(self, *args, **options):
        """
        Updates the column ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe due to the incorrect
        mapping in settings.py
        """
        ds_cursor = connection.cursor()
        logger.info('Begin updating file B and C')

        broker_cols_b = self.get_list_of_broker_cols(financial_accounts_awards)
        broker_cols_type_b = self.get_list_of_broker_cols_types(financial_accounts_awards)
        website_cols_b = self.get_website_row_formatted(financial_accounts_awards)

        broker_cols_c = self.get_list_of_broker_cols(financial_accounts_oc)
        broker_cols_type_c = self.get_list_of_broker_cols_types(financial_accounts_oc)
        website_cols_c = self.get_website_row_formatted(financial_accounts_oc)

        logger.info('Getting submission ids to update')
        start = timeit.default_timer()
        submissions_to_update = self.get_list_of_submissions()
        logger.info('Finished retrieving submission in {} seconds'.format(str(start-timeit.default_timer())))

        for submission in submissions_to_update:
            submission_id = submission[0]
            logger.info('Loading rows data to update File B submission {}'.format(submission_id))

            start = timeit.default_timer()
            logger.info('Retrieving rows to update for File B submission {}'.format(submission_id))
            get_rows_to_update_query = self.get_rows_to_update('B', submission_id,
                                                               broker_cols_b, broker_cols_type_b,
                                                               website_cols_b)
            ds_cursor.execute(get_rows_to_update_query)
            logger.info('Finished retrieving rows to update for File B submission {} in {} seconds'.format(
                submission_id, str(timeit.default_timer()-start)
            ))

            start = timeit.default_timer()
            logger.info('Retrieving rows to update for File B submission {}'.format(submission_id))
            update_rows = self.update_website_rows(
                'financial_accounts_by_awards', 'file_b_rows_to_update', financial_accounts_awards
            )
            ds_cursor.execute(update_rows)
            logger.info('Finished updating Rows for File B table submission {} in {} seconds'.format(
                submission_id, str(timeit.default_timer()-start)
            ))

            start = timeit.default_timer()
            logger.info('Retrieving rows to update for File B submission {}'.format(submission_id))
            get_rows_to_update_query = self.get_rows_to_update('C', submission_id,
                                                               broker_cols_c, broker_cols_type_c,
                                                               website_cols_c
                                                               )
            ds_cursor.execute(get_rows_to_update_query)
            logger.info('Finished retrieving rows to update for File C submission {} in {} seconds'.format(
                submission_id, str(timeit.default_timer()-start)
            ))

            start = timeit.default_timer()
            logger.info('Retrieving rows to update for File C submission {}'.format(submission_id))
            update_rows = self.update_website_rows(
                'financial_accounts_by_program_activity_object_class',
                'file_c_rows_to_update', financial_accounts_oc
            )
            ds_cursor.execute(update_rows)
            logger.info('Finished updating Rows for File C table submission {} in {} seconds'.format(
                submission_id, str(timeit.default_timer()-start)
            ))

            ds_cursor.execute("DROP TABLE file_b_rows_to_update")
            ds_cursor.execute("DROP TABLE file_c_rows_to_update")

        logger.info('Done updating file B and C mappings')
