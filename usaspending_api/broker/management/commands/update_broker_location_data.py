import logging
from datetime import datetime
import time

from django.db import connection
from django.core.management.base import BaseCommand, CommandError

from usaspending_api.etl.management.load_base import run_sql_file

logger = logging.getLogger('console')


class Command(BaseCommand):
    help = "Update specific transactions with data from broker and its related tables"

    fabs_columns = [
        'published_award_financial_assistance_id',
        'afa_generated_unique',
        'legal_entity_address_line1',
        'legal_entity_address_line2',
        'legal_entity_address_line3',
        'legal_entity_city_name',
        'legal_entity_city_code',
        'legal_entity_congressional',
        'legal_entity_country_code',
        'legal_entity_country_name',
        'legal_entity_county_code',
        'legal_entity_county_name',
        'legal_entity_foreign_city',
        'legal_entity_foreign_posta',
        'legal_entity_foreign_provi',
        'legal_entity_state_code',
        'legal_entity_state_name',
        'legal_entity_zip5',
        'legal_entity_zip_last4',
        'place_of_performance_city',
        'place_of_performance_code',
        'place_of_performance_congr',
        'place_of_perform_country_c',
        'place_of_perform_country_n',
        'place_of_perform_county_co',
        'place_of_perform_county_na',
        'place_of_performance_forei',
        'place_of_perform_state_nam',
        'place_of_performance_zip4a',
        # 'place_of_perfor_state_code',
        # 'place_of_performance_zip5',
        # 'place_of_perform_zip_last4'
    ]

    fpds_columns = [
        'detached_award_procurement_id',
        'detached_award_proc_unique',
        'legal_entity_address_line1',
        'legal_entity_address_line2',
        'legal_entity_address_line3',
        'legal_entity_city_name',
        'legal_entity_congressional',
        'legal_entity_country_code',
        'legal_entity_country_name',
        # 'legal_entity_county_code',
        # 'legal_entity_county_name',
        'legal_entity_state_code',
        'legal_entity_state_descrip',
        # 'legal_entity_zip5',
        'legal_entity_zip4',
        # 'legal_entity_zip_last4',
        'place_of_perform_city_name',
        'place_of_performance_congr',
        'place_of_perform_country_c',
        'place_of_perf_country_desc',
        # 'place_of_perform_county_co',
        'place_of_perform_county_na',
        'place_of_performance_state',
        'place_of_perfor_state_desc',
        # 'place_of_performance_zip5',
        'place_of_performance_zip4a',
        # 'place_of_perform_zip_last4'
    ]

    def set_db_values(self, file_type):
        if file_type == 'fpds':
            database_columns = self.fpds_columns
            broker_table = 'detached_award_procurement'
            website_table = f'transaction_{file_type}'
            unique_identifier = 'detached_award_proc_unique'
        else:
            database_columns = self.fabs_columns
            broker_table = 'published_award_financial_assistance'
            website_table = f'transaction_{file_type}'
            unique_identifier = 'afa_generated_unique'

        return database_columns, broker_table, website_table, unique_identifier

    def add_arguments(self, parser):
        parser.add_argument(
            '--fiscal_year',
            type=int,
            help='Fiscal year to chose to pull from Broker'
        )

        parser.add_argument(
            '--assistance',
            action='store_const',
            dest='file_type',
            const='fabs',
            help='Updates FABS location data'
        )

        parser.add_argument(
            '--contracts',
            action='store_const',
            dest='file_type',
            const='fpds',
            help='Updates FPDS location data'
        )

    def handle(self, *args, **options):
        fiscal_year = options.get('fiscal_year')
        file_type = options.get('file_type')
        ds_cursor = connection.cursor()

        if file_type is None:
            raise CommandError('Must specify --contracts or --assistance')

        if not fiscal_year:
            raise CommandError('Must specify --fiscal_year')

        fy_start = '10/01/' + str(fiscal_year - 1)
        fy_end = '09/30/' + str(fiscal_year)

        database_columns, broker_table, website_table, unique_identifier = self.set_db_values(file_type)

        # Fetches rows that need to be updated based on batches pulled from the cursor
        start = datetime.now()
        logger.info('Fetching rows to update from broker for FY{} {} data'.format(fiscal_year, file_type.upper()))

        ds_cursor.execute(
            get_data_to_update_from_broker(file_type, database_columns, broker_table, website_table, fy_start, fy_end, unique_identifier)
        )

        ds_cursor.execute(
                update_tmp_table_location_changes(file_type, database_columns, unique_identifier, website_table)
        )

        # Retrieves temporary table with FABS rows that need to be updated
        ds_cursor.execute('SELECT count(*) from {}_transactions_to_update_location;'.format(file_type))
        db_rows = ds_cursor.fetchall()[0][0]

        logger.info("Completed fetching {} rows to update in {} seconds".format(db_rows, datetime.now()-start))

        start = datetime.now()
        if db_rows > 0:
            ds_cursor.execute(update_transaction_table(file_type, database_columns, website_table, unique_identifier))
            run_sql_file('usaspending_api/broker/management/sql/update_{}_location_data.sql'.format(file_type), {})

        logger.info("Completed updating: {} {} rows in {} seconds".format(file_type.upper(), db_rows, datetime.now() - start))
        ds_cursor.execute('DROP TABLE {}_transactions_to_update_location;'.format(file_type))


def get_data_to_update_from_broker(file_type, database_columns, broker_table, website_table, fy_start, fy_end,
                                   unique_identifier):
        is_active = 'is_active = TRUE and' if file_type == 'fabs' else ''
        location_columns = " ,".join(database_columns)
        location_columns_type = " ,".join(["{} text".format(column) for column in database_columns])

        get_data_broker = f"""
           CREATE TEMPORARY TABlE {file_type}_transactions_to_update_location AS
           SELECT * from dblink('broker_server','
           SELECT
               {location_columns}
               from {broker_table}
               where {is_active} action_date::date >= ''{fy_start}''::date and
               action_date::date <= ''{fy_end}''::date;
               ') AS (
               {location_columns_type}
               )
              EXCEPT
              SELECT
              {location_columns}
               from {website_table}
               where action_date::date >= '{fy_start}'::date and
               action_date::date <= '{fy_end}'::date;

            -- Adding index to table to improve speed
           CREATE INDEX {file_type}_unique_idx ON {file_type}_transactions_to_update_location({unique_identifier});

           """
        return get_data_broker


def update_tmp_table_location_changes(file_type, database_columns, unique_identifier, website_table):
    le_loc_columns_distinct = " OR ".join(['website.{column} IS DISTINCT FROM broker.{column}'.format(column=column)
                                          for column in database_columns if column[:12] == 'legal_entity'])

    pop_loc_columns_distinct = " OR ".join(['website.{column} IS DISTINCT FROM broker.{column}'.format(column=column)
                                           for column in database_columns if column[:13] == 'place_of_perf'])

    update_loc_changes_tmp_table = f"""
         -- Include columns to determine whether we need a place of performance change or recipient location
       ALTER TABLE {file_type}_transactions_to_update_location
       ADD COLUMN pop_change boolean, add COLUMN le_loc_change boolean;

        UPDATE {file_type}_transactions_to_update_location broker
        SET
        le_loc_change = (
            CASE  WHEN
            {le_loc_columns_distinct}
            THEN TRUE ELSE FALSE END
            ),
        pop_change = (
            CASE  WHEN
           {pop_loc_columns_distinct}
           THEN TRUE ELSE FALSE END
           )
        FROM {website_table} website
        WHERE broker.{unique_identifier} = website.{unique_identifier};


        -- Delete rows where there is no transaction in the table
        DELETE FROM {file_type}_transactions_to_update_location where pop_change is null and le_loc_change is null;

        -- Adding index to table to improve speed on update
        CREATE INDEX {file_type}_le_loc_idx ON {file_type}_transactions_to_update_location(le_loc_change);
        CREATE INDEX {file_type}_pop_idx ON {file_type}_transactions_to_update_location(pop_change);
        ANALYZE {file_type}_transactions_to_update_location;
        """
    return update_loc_changes_tmp_table


def update_transaction_table(file_type, database_columns, website_table, unique_identifier):
    update_website_rows = " ,".join(['{column} = broker.{column}'.format(column=column)
                                     for column in database_columns[2:]]
                                    )
    update_website_tables = f"""
            UPDATE {website_table} as website
            SET
                {update_website_rows}
            FROM
                {file_type}_transactions_to_update_location AS broker
            WHERE
                broker.{unique_identifier} = website.{unique_identifier};

            """

    return update_website_tables
