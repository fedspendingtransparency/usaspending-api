import logging
import os

from django.core.management.base import BaseCommand
from django.db import transaction
from functools import partial
from psycopg2.sql import Identifier, Literal, SQL
from usaspending_api.common.helpers.sql_helpers import convert_composable_query_to_string, get_connection
from usaspending_api.common.helpers.timing_helpers import Timer


logger = logging.getLogger("console")


# All of our timers log to logger.info and logger.error.
Timer = partial(Timer, success_logger=logger.info, failure_logger=logger.error)


class Command(BaseCommand):
    help = "Load subawards from Broker into USAspending."

    def __init__(self, *args, **kwargs):
        self.full_reload = False
        self.log_sql = False
        super().__init__(*args, **kwargs)

    def add_arguments(self, parser):
        parser.add_argument(
            "--full-reload",
            action="store_true",
            help="Empties the USAspending subaward and broker_subaward tables before loading.",
        )

        parser.add_argument(
            "--sql",
            action="store_true",
            help="For debugging.  Log all SQL statements.  More verbose than you might expect.",
        )

    def handle(self, *args, **options):

        # Pick out just the last bit of the module name (which should match the
        # file name minus the extension).
        module_name = __name__.split(".")[-1]
        with Timer(module_name):

            self.full_reload = options["full_reload"]
            self.log_sql = options["sql"]

            if self.full_reload:
                logger.info("--full-reload flag supplied.  Performing a full reload.")
                where = ""

            else:
                max_id, max_created_at, max_updated_at = self._get_maxes()

                if max_id is None and max_created_at is None and max_updated_at is None:
                    logger.info("USAspending's broker_subaward table is empty.  Promoting to full reload.")
                    self.full_reload = True
                    where = ""

                else:
                    logger.info(
                        "Performing an incremental load on USAspending's broker_subaward "
                        "where id > {}, created_at > {}, or "
                        "updated_at > {}".format(max_id, max_created_at, max_updated_at)
                    )
                    where = self._build_where(id=max_id, created_at=max_created_at, updated_at=max_updated_at)

            self._perform_load(where)

    def _execute_sql(self, sql):
        """
        Pretty straightforward.  Executes some SQL.
        """
        if self.log_sql:
            logger.info(sql)

        connection = get_connection(read_only=False)
        with connection.cursor() as cursor:
            cursor.execute(sql)
            rowcount = cursor.rowcount
            if rowcount > -1:
                logger.info("{:,} rows affected".format(rowcount))

    def _execute_sql_file(self, filename, where=None):
        """
        Read in a SQL file, perform any injections, execute the results.
        """
        filepath = os.path.join(os.path.split(__file__)[0], "load_subawards_sql", filename)

        with open(filepath) as f:
            sql = f.read()

        if where is not None:
            sql = sql.format(where=where)

        with Timer(filename):
            self._execute_sql(sql)

    def _get_maxes(self):
        """
        Get some values from the broker_subaward table that we can use to
        identify new/updated records in Broker.
        """
        sql = "select max(id), max(created_at), max(updated_at) from broker_subaward"

        if self.log_sql:
            logger.info(sql)

        with Timer("Retrieve incremental values from broker_subaward"):
            connection = get_connection()
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall()[0]

    def _perform_load(self, where):
        """
        The actual heavy lifting.  Call the SQLs necessary to load subawards.
        """
        if not self.full_reload:
            self._execute_sql_file("010_find_new_awards.sql")

        with transaction.atomic():
            if self.full_reload:
                with Timer("delete from broker_subaward"):
                    self._execute_sql("delete from broker_subaward")
            self._execute_sql_file("020_import_broker_subawards.sql", where=where)

        self._execute_sql_file("030_frame_out_subawards.sql")
        self._execute_sql_file("040_enhance_with_awards_data.sql")
        self._execute_sql_file("050_enhance_with_transaction_data.sql")
        self._execute_sql_file("060_enhance_with_cfda_data.sql")
        self._execute_sql_file("070_enhance_with_awarding_agency.sql")
        self._execute_sql_file("080_enhance_with_funding_agency.sql")
        self._execute_sql_file("090_create_temp_address_table.sql")
        self._execute_sql_file("100_enhance_with_pop_county_city.sql")
        self._execute_sql_file("110_enhance_with_pop_country.sql")
        self._execute_sql_file("120_enhance_with_recipient_location_county_city.sql")
        self._execute_sql_file("130_enhance_with_recipient_location_country.sql")

        # For update_award_totals, if we're performing a full reload, update
        # all awards that have subawards, otherwise, if this is an incremental
        # update, just update those awards that have may have been affected.
        if self.full_reload:
            where = "where award_id is not null"
        else:
            where = (
                "where award_id in (select award_id from temp_load_subawards_subaward where award_id is not null)"
            )

        with transaction.atomic():
            if self.full_reload:
                with Timer("delete from subaward"):
                    self._execute_sql("delete from subaward")
            self._execute_sql_file("140_add_subawards.sql")
            if self.full_reload:
                self._execute_sql_file("150_reset_unlinked_award_subaward_fields.sql")
            self._execute_sql_file("160_update_award_subaward_fields.sql", where=where)

        # We'll do this outside of the transaction since it doesn't hurt
        # anything to reimport subawards.
        self._execute_sql_file("170_mark_imported.sql")

        self._execute_sql_file("900_cleanup.sql")

    @staticmethod
    def _build_where(**where_columns):
        """
        Accepts a keyword arguments that are valid subaward columns names and
        the value upon which they will be filtered.  Will return a SQL where
        clause along the lines of:

            where column1 > value1 or column2 > value2

        Uses psycopg Identifier, Literal, and SQL to ensure everything is
        properly formatted.
        """
        wheres = []
        for column, value in where_columns.items():
            if value is not None:
                wheres.append(SQL("{} > {}").format(Identifier(column), Literal(value)))

        if wheres:
            # Because our where clause is embedded in a dblink, we need to
            # wrap it in a literal again to get everything escaped properly.
            where = SQL("where {}").format(SQL(" or ").join(wheres))
            where = Literal(convert_composable_query_to_string(where))
            where = convert_composable_query_to_string(where)
            return where[1:-1]  # Remove outer quotes

        return ""
