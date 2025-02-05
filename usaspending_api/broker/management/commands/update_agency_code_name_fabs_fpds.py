import logging
import time
import datetime

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import connection

logger = logging.getLogger("script")


class Command(BaseCommand):
    help = "Update Agency codes from broker that are 999 in the website or that are associated with a certain sub tier"

    @staticmethod
    def get_broker_data(table_type, fiscal_year, fy_start, fy_end, year_range=None, sub_tiers=None):
        # Base WHERE clauses
        broker_where = """action_date::date >= ''{fy_start}''::date
            AND action_date::date <= ''{fy_end}''::date""".format(
            fy_start=fy_start, fy_end=fy_end
        )
        usaspending_where = """action_date::date >= '{fy_start}'::date
        AND action_date::date <= '{fy_end}'::date""".format(
            fy_start=fy_start, fy_end=fy_end
        )

        # If we're doing everything before a certain fiscal year
        if year_range == "pre":
            broker_where = "action_date::date < ''{fy_start}''::date".format(fy_start=fy_start)
            usaspending_where = "action_date::date < '{fy_start}'::date".format(fy_start=fy_start)
            fiscal_year = "pre_" + str(fiscal_year)

        # If we're doing everything after a certain fiscal year
        if year_range == "post":
            broker_where = "action_date::date > ''{fy_end}''::date".format(fy_end=fy_end)
            usaspending_where = "action_date::date > '{fy_end}'::date".format(fy_end=fy_end)
            fiscal_year = "post_" + str(fiscal_year)

        table = "detached_award_procurement"
        unique = "detached_award_proc_unique"
        if table_type == "fabs":
            broker_where = "is_active IS TRUE AND " + broker_where
            table = "published_fabs"
            unique = "afa_generated_unique"

        if sub_tiers and len(sub_tiers) == 1:
            subtier_cond = "(awarding_sub_tier_agency_c = {sub_tier} OR funding_sub_tier_agency_co = {sub_tier})"
            broker_where += " AND " + subtier_cond.format(sub_tier=sub_tiers[0])
        elif sub_tiers and len(sub_tiers) > 1:
            sub_tiers_str = "({})".format(",".join(["''{}''".format(sub_tier) for sub_tier in sub_tiers]))
            subtier_cond = "(awarding_sub_tier_agency_c IN {sub_tiers} OR funding_sub_tier_agency_co IN {sub_tiers})"
            broker_where += " AND " + subtier_cond.format(sub_tiers=sub_tiers_str)
        broker_where += ";"
        usaspending_where += ";"

        sql_statement = """
        CREATE TEMPORARY TABLE {table_type}_agencies_to_update_{fy} AS
        SELECT * FROM dblink('{broker_server}',
        '
        SELECT
            {table}_id,
            UPPER({unique}) AS {unique},
            UPPER(awarding_agency_code) AS awarding_agency_code,
            UPPER(awarding_agency_name) AS awarding_agency_name,
            UPPER(funding_agency_code) AS funding_agency_code,
            UPPER(funding_agency_name) AS funding_agency_name
        FROM {table}
            WHERE {broker_where}
            ')
        AS (
            {table}_id  integer,
            {unique}  text,
            awarding_agency_code  text,
            awarding_agency_name text,
            funding_agency_code  text,
            funding_agency_name text
            )
       EXCEPT
        SELECT
            {table}_id,
            {unique},
            awarding_agency_code,
            awarding_agency_name,
            funding_agency_code,
            funding_agency_name
        FROM transaction_{table_type}
        WHERE {usaspending_where}
        -- Adding Indexes
        CREATE INDEX {table_type}_unique_id_index_{fy} ON {table_type}_agencies_to_update_{fy}({unique});
        ANALYZE {table_type}_agencies_to_update_{fy};
        """.format(
            table_type=table_type,
            table=table,
            unique=unique,
            fy=fiscal_year,
            broker_where=broker_where,
            usaspending_where=usaspending_where,
            broker_server=settings.DATA_BROKER_DBLINK_NAME,
        )
        return sql_statement

    @staticmethod
    def update_website(fiscal_year, table_type, sub_tiers=None, year_range=None):
        award_where = "awarding_agency_code = '999'"
        fund_where = "funding_agency_code = '999'"
        if sub_tiers and len(sub_tiers) == 1:
            award_where = "awarding_sub_tier_agency_c = '{}'".format(sub_tiers)
            fund_where = "funding_sub_tier_agency_co = '{}'".format(sub_tiers)
        elif sub_tiers and len(sub_tiers) > 1:
            sub_tiers_str = "({})".format(",".join(["'{}'".format(sub_tier) for sub_tier in sub_tiers]))
            award_where = "awarding_sub_tier_agency_c IN {}".format(sub_tiers_str)
            fund_where = "funding_sub_tier_agency_co IN {}".format(sub_tiers_str)

        # if there's a range we add it to the name of the table
        if year_range:
            fiscal_year = year_range + "_" + str(fiscal_year)

        # Setting the unique key depending on type
        unique = "detached_award_proc_unique"
        if table_type == "fabs":
            unique = "afa_generated_unique"

        sql_statement = """
        -- Updating awarding agency code
        UPDATE transaction_{table_type}
        SET
            awarding_agency_code = broker.awarding_agency_code,
            awarding_agency_name = broker.awarding_agency_name
        FROM
            {table_type}_agencies_to_update_{fiscal_year} broker
        WHERE
            transaction_{table_type}.{unique} = broker.{unique}
            AND
            transaction_{table_type}.{award_where};
        -- Updating funding agency code
        UPDATE transaction_{table_type}
        SET
            funding_agency_code = broker.funding_agency_code,
            funding_agency_name = broker.funding_agency_name
        FROM
            {table_type}_agencies_to_update_{fiscal_year} broker
        WHERE
            transaction_{table_type}.{unique} = broker.{unique}
            AND
            transaction_{table_type}.{fund_where};
        """.format(
            fiscal_year=fiscal_year,
            table_type=table_type,
            unique=unique,
            award_where=award_where,
            fund_where=fund_where,
        )

        return sql_statement

    def run_updates(self, fiscal_year, table_type, sub_tiers, year_range=None):
        """
        Run the actual updates
        """

        db_cursor = connection.cursor()

        fy_start = "10/01/" + str(fiscal_year - 1)
        fy_end = "09/30/" + str(fiscal_year)

        logger.info(
            "Retrieving {} rows to update from broker for {}FY{}".format(
                table_type.upper(), year_range or "", fiscal_year
            )
        )
        start = time.perf_counter()

        # Comparing broker rows with website for a specific fiscal year
        db_cursor.execute(self.get_broker_data(table_type, fiscal_year, fy_start, fy_end, year_range, sub_tiers))

        end = time.perf_counter()
        condition_str = " with sub_tiers {}".format(sub_tiers) if sub_tiers else ""
        logger.info(
            "Finished retrieving {}FY{} data from broker {}{} to update in website in {}s".format(
                year_range or "", fiscal_year, table_type.upper(), condition_str, end - start
            )
        )

        logger.info("Updating transaction_{} rows agency codes and names".format(table_type))
        start = time.perf_counter()

        # Updates website rows with agency code 999
        db_cursor.execute(self.update_website(fiscal_year, table_type, sub_tiers, year_range))

        end = time.perf_counter()
        logger.info(
            "Finished updating {}FY{} transaction {} rows in {}s".format(
                year_range or "", fiscal_year, table_type, end - start
            )
        )

    def process_single_year(self, year, table_types, sub_tiers):
        """Process single year"""
        for table_type in table_types:
            self.run_updates(year, table_type, sub_tiers)

    def process_multiple_years(self, table_types, sub_tiers):
        """Process all years option"""
        curr_year = datetime.datetime.now().year
        year_list = [i for i in range(2000, curr_year + 1)]

        for table_type in table_types:
            self.run_updates(year_list[0], table_type, sub_tiers, year_range="pre")

        for fy in year_list:
            for table_type in table_types:
                self.run_updates(fy, table_type, sub_tiers)

        for table_type in table_types:
            self.run_updates(year_list[-1], table_type, sub_tiers, year_range="post")

    def add_arguments(self, parser):
        parser.add_argument(
            "--fiscal-year",
            type=int,
            dest="fiscal-year",
            help="Fiscal year to pull from Broker. If not provided, pulls all years",
        )
        parser.add_argument(
            "--type", choices=["fabs", "fpds", "both"], default="both", help="Which table to make corrections for"
        )
        parser.add_argument(
            "--sub-tiers",
            type=str,
            nargs="+",
            dest="sub-tiers",
            help="Sub tiers to update agencies related to if this is to be used for sub tier changes",
        )

    def handle(self, *args, **options):
        """
        Updates the agency codes in the website transaction tables from broker where code or name don't match
        """
        fiscal_year = options.get("fiscal-year")
        table_option = options.get("type")
        sub_tiers = options.get("sub-tiers")

        if sub_tiers:
            for sub_tier in sub_tiers:
                if len(sub_tier) != 4:
                    raise CommandError("When provided, sub tier code must be 4 characters long.")

        logger.info("Starting script to update agency codes from broker")

        table_types = []
        if table_option in ("fpds", "both"):
            table_types.append("fpds")
        if table_option in ("fabs", "both"):
            table_types.append("fabs")

        if fiscal_year:
            self.process_single_year(fiscal_year, table_types, sub_tiers)
        else:
            self.process_multiple_years(table_types, sub_tiers)
