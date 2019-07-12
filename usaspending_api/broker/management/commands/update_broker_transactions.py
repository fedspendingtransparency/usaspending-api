import logging
from datetime import datetime

from django.db import connection
from django.core.management.base import BaseCommand, CommandError

from usaspending_api.broker.management.commands.update_broker_location_data import (
    update_tmp_table_location_changes,
    update_location_table,
)

logger = logging.getLogger("console")


class Command(BaseCommand):
    help = "Update specific transactions with data from broker and its related tables"

    # Lists can be updated to change which columns to update
    fabs_columns = [
        "legal_entity_congressional",
        "legal_entity_country_code",
        "legal_entity_country_name",
        "legal_entity_county_code",
        "legal_entity_county_name",
        "legal_entity_state_code",
        "legal_entity_state_name",
        "legal_entity_zip5",
        "legal_entity_zip_last4",
        "place_of_performance_congr",
        "place_of_perform_country_c",
        "place_of_perform_country_n",
        "place_of_perform_county_co",
        "place_of_perform_county_na",
        "place_of_perform_state_nam",
        "place_of_perfor_state_code",
        "place_of_performance_zip4a",
        "place_of_performance_zip5",
        "place_of_perform_zip_last4",
    ]

    fpds_columns = [
        "legal_entity_congressional",
        "legal_entity_country_code",
        "legal_entity_country_name",
        "legal_entity_county_code",
        "legal_entity_county_name",
        "legal_entity_state_code",
        "legal_entity_state_descrip",
        "legal_entity_zip5",
        "legal_entity_zip4",
        "legal_entity_zip_last4",
        "place_of_performance_congr",
        "place_of_perform_country_c",
        "place_of_perf_country_desc",
        "place_of_perform_county_co",
        "place_of_perform_county_na",
        "place_of_performance_state",
        "place_of_perfor_state_desc",
        "place_of_performance_zip5",
        "place_of_performance_zip4a",
        "place_of_perform_zip_last4",
    ]

    def set_db_values(self, file_type):
        if file_type == "fpds":
            database_columns = self.fpds_columns
            broker_table = "detached_award_procurement"
            unique_identifier = "detached_award_proc_unique"
        else:
            database_columns = self.fabs_columns
            broker_table = "published_award_financial_assistance"
            unique_identifier = "afa_generated_unique"

        return database_columns, broker_table, unique_identifier

    def add_arguments(self, parser):
        parser.add_argument("--fiscal_year", type=int, help="Fiscal year to chose to pull from Broker")

        parser.add_argument(
            "--assistance", action="store_const", dest="file_type", const="fabs", help="Updates FABS data"
        )

        parser.add_argument(
            "--contracts", action="store_const", dest="file_type", const="fpds", help="Updates FPDS data"
        )

        parser.add_argument(
            "--location",
            action="store_true",
            dest="update_location",
            default=False,
            help="Updates references location table (optional)",
        )

    def handle(self, *args, **options):
        fiscal_year = options.get("fiscal_year")
        file_type = options.get("file_type")
        ds_cursor = connection.cursor()

        if file_type is None:
            raise CommandError("Must specify --contracts or --assistance")

        if not fiscal_year:
            raise CommandError("Must specify --fiscal_year")

        fy_start = "10/01/" + str(fiscal_year - 1)
        fy_end = "09/30/" + str(fiscal_year)

        database_columns, broker_table, unique_identifier = self.set_db_values(file_type)

        # Fetches rows that need to be updated based on batches pulled from the cursor
        start = datetime.now()
        logger.info("Fetching rows to update from broker for FY{} {} data".format(fiscal_year, file_type.upper()))

        ds_cursor.execute(
            get_data_to_update_from_broker(
                file_type, database_columns, broker_table, fiscal_year, fy_start, fy_end, unique_identifier
            )
        )

        # If the user specifies location tag, will additional columns and indexing to the temporary table
        # Separates which rows need the legal entity updated from place of performance
        if options.get("update_location"):
            ds_cursor.execute(
                update_tmp_table_location_changes(file_type, database_columns, unique_identifier, fiscal_year)
            )

        # Retrieves temporary table with FABS rows that need to be updated
        ds_cursor.execute("SELECT count(*) from {}_transactions_to_update_{};".format(file_type, fiscal_year))
        db_rows = ds_cursor.fetchall()[0][0]

        logger.info("Completed fetching {} rows to update in {} seconds".format(db_rows, datetime.now() - start))

        start = datetime.now()
        if db_rows > 0:
            # Updates the transactions_fpds or transaction_fabs with data from broker
            ds_cursor.execute(update_transaction_table(file_type, database_columns, unique_identifier, fiscal_year))

            # Updates references_location table when the user specifies --location
            if options.get("update_location"):
                ds_cursor.execute(
                    update_location_table(file_type, "recipient", database_columns, unique_identifier, fiscal_year)
                )
                ds_cursor.execute(
                    update_location_table(
                        file_type, "place_of_performance", database_columns, unique_identifier, fiscal_year
                    )
                )

        logger.info(
            "Completed updating: {} {} rows in {} seconds".format(file_type.upper(), db_rows, datetime.now() - start)
        )


def get_data_to_update_from_broker(
    file_type, database_columns, broker_table, fiscal_year, fy_start, fy_end, unique_identifier
):
    """
    Generates SQL script to pull data from broker and compare it with website table
    Creates Temporary table of the rows that differ between the two databases
    """
    is_active = "is_active = TRUE and" if file_type == "fabs" else ""
    columns = " ,".join(database_columns)
    columns_type = " ,".join(["{} text".format(column) for column in database_columns])

    sql_statement = """
       CREATE TEMPORARY TABlE {file_type}_transactions_to_update_{fiscal_year} AS
       SELECT * from dblink('broker_server','
       SELECT
           {unique_identifier},
           {columns}
           FROM {broker_table}
           WHERE {is_active} action_date:: date >= ''{fy_start}'':: date AND
           action_date:: date <= ''{fy_end}'':: date;
           ') AS (
           {unique_identifier} text,
           {columns_type}
           )
          EXCEPT
          SELECT
          {unique_identifier},
          {columns}
           FROM transaction_{file_type}
           WHERE action_date:: date >= '{fy_start}':: date AND
           action_date:: date <= '{fy_end}':: date;
        -- Adding index to table to improve speed
       CREATE INDEX {file_type}_unique_idx ON {file_type}_transactions_to_update_{fiscal_year}({unique_identifier});
       """.format(
        file_type=file_type,
        fiscal_year=fiscal_year,
        unique_identifier=unique_identifier,
        columns=columns,
        broker_table=broker_table,
        is_active=is_active,
        fy_start=fy_start,
        fy_end=fy_end,
        columns_type=columns_type,
    )
    return sql_statement


def update_transaction_table(file_type, database_columns, unique_identifier, fiscal_year):
    """
    Generates SQL script to update the website table with the  data from broker
    """
    update_website_rows = " ,".join(
        ["{column} = broker.{column}".format(column=column) for column in database_columns[2:]]
    )
    sql_statement = """
            UPDATE transaction_{file_type} AS website
            SET
                {update_website_rows}
            FROM
                {file_type}_transactions_to_update_{fiscal_year} AS broker
            WHERE
                broker.{unique_identifier} = website.{unique_identifier};
            """.format(
        file_type=file_type,
        fiscal_year=fiscal_year,
        unique_identifier=unique_identifier,
        update_website_rows=update_website_rows,
    )

    return sql_statement
