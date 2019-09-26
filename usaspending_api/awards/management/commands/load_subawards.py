import logging

from collections import namedtuple
from django.core.management.base import BaseCommand
from django.db import transaction
from pathlib import Path
from psycopg2.sql import Identifier, Literal, SQL
from usaspending_api.common.helpers.sql_helpers import convert_composable_query_to_string, get_connection
from usaspending_api.common.helpers.timing_helpers import Timer


logger = logging.getLogger("console")


class Command(BaseCommand):

    help = "Load subawards from Broker into USAspending."
    full_reload = False
    log_sql = False

    def add_arguments(self, parser):
        parser.add_argument(
            "--full-reload",
            action="store_true",
            default=self.full_reload,
            help="Empties the USAspending subaward and broker_subaward tables before loading.",
        )

        parser.add_argument(
            "--log-sql",
            action="store_true",
            default=self.log_sql,
            help="For debugging.  Log all SQL statements.  More verbose than you might expect.",
        )

    def handle(self, *args, **options):

        with Timer("Load subawards"):

            self.full_reload = options["full_reload"]
            self.log_sql = options["log_sql"]

            where = self._prepare_where_clause()

            # There are a couple of situations where we may need to automatically promote an
            # incremental reload to a full reload.  An empty where clause implies that this
            # needs to happen.
            if where == "":
                self.full_reload = True

            try:
                with transaction.atomic():
                    self._perform_load(where)
            except Exception:
                logger.error("ALL CHANGES WERE ROLLED BACK DUE TO EXCEPTION")
                raise

    def _prepare_where_clause(self):
        where = ""

        if self.full_reload:
            logger.info("--full-reload flag supplied.  Performing a full reload.")

        else:
            with Timer("Retrieve delta values"):
                mm = self._get_mins_and_maxes()

                logger.info(
                    "FOUND broker_min_id={0.broker_min_id}, local_min_id={0.local_min_id}, "
                    "local_max_id={0.local_max_id}, local_max_created_at={0.local_max_created_at}, "
                    "local_max_updated_at={0.local_max_updated_at}".format(mm)
                )

            if mm.broker_min_id is None:
                raise RuntimeError("Unable to determine minimum id from Broker subaward table.  No value returned.")

            if mm.local_max_id is None and mm.local_max_created_at is None and mm.local_max_updated_at is None:
                logger.info("USAspending's broker_subaward table is empty.  Promoting to full reload.")

            # Automatic reload detection.  We can take advantage of the fact that Broker's subaward
            # ids are serial autonumbers by checking for the lowest id from both tables and promoting
            # to a full reload if we detect a difference.
            elif mm.local_min_id != mm.broker_min_id:
                logger.info(
                    "Broker's subaward table appears to have been reloaded.  Promoting to full reload.  "
                    "(min id {0.local_min_id} != {0.broker_min_id})".format(mm)
                )

            else:
                logger.info(
                    "Performing an incremental load on USAspending's broker_subaward "
                    "where id > {0.local_max_id}, created_at > {0.local_max_created_at}, or "
                    "updated_at > {0.local_max_updated_at}".format(mm)
                )
                where = self._build_where(
                    id=mm.local_max_id, created_at=mm.local_max_created_at, updated_at=mm.local_max_updated_at
                )

        return where

    def _execute_sql(self, sql, fetcher=None):
        """
        Pretty straightforward.  Executes some SQL.  If a fetcher function is provided, returns the
        result of calling the fetcher.  Be careful with fetchers.  Supplying a fetcher that
        restructures results along with a query that does not return results will cause an exception.
        """
        if self.log_sql:
            logger.info(sql)

        connection = get_connection(read_only=False)
        with connection.cursor() as cursor:
            cursor.execute(sql)
            rowcount = cursor.rowcount
            if rowcount > -1:
                logger.info("{:,} rows {}".format(rowcount, "affected" if fetcher is None else "returned"))
            if fetcher is not None:
                return fetcher(cursor)

    def _execute_sql_file(self, filename, where=None, fetcher=None):
        """ Read in a SQL file, perform any injections, execute the results. """
        filepath = Path(__file__).resolve().parent / "load_subawards_sql" / filename
        sql = filepath.read_text()

        if where is not None:
            sql = sql.format(where=where)

        with Timer(filename):
            self._execute_sql(sql, fetcher=fetcher)

    def _get_mins_and_maxes(self):
        """
        Get some preliminary data that will help us to identify records that require
        adding or updating.
        """
        Results = namedtuple(
            "MinsAndMaxes",
            ["broker_min_id", "local_min_id", "local_max_id", "local_max_created_at", "local_max_updated_at"],
        )

        broker = self._execute_sql(
            "select t.min_id from dblink('broker_server', 'select min(id) from subaward') as t (min_id integer)",
            lambda c: c.fetchall()[0],  # Returns the first row
        )

        local = self._execute_sql(
            "select min(id), max(id), max(created_at), max(updated_at) from broker_subaward",
            lambda c: c.fetchall()[0],  # Returns the first row
        )

        return Results(
            broker_min_id=broker[0],
            local_min_id=local[0],
            local_max_id=local[1],
            local_max_created_at=local[2],
            local_max_updated_at=local[3],
        )

    def _perform_load(self, where):
        """ The actual heavy lifting.  Call the SQLs necessary to load subawards. """
        if self.full_reload:
            with Timer("delete from broker_subaward"):
                self._execute_sql("delete from broker_subaward")
        else:
            self._execute_sql_file("010_find_new_awards.sql")

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

        if self.full_reload:
            with Timer("delete from subaward"):
                self._execute_sql("delete from subaward")

        self._execute_sql_file("140_add_subawards.sql")

        if self.full_reload:
            self._execute_sql_file("150_reset_unlinked_award_subaward_fields.sql")

        # For update_award_totals, if we're performing a full reload, update
        # all awards that have subawards, otherwise, if this is an incremental
        # update, just update those awards that have may have been affected.
        if self.full_reload:
            where = "where award_id is not null"
        else:
            where = (
                "where award_id in (select distinct award_id from temp_load_subawards_subaward "
                "where award_id is not null)"
            )

        self._execute_sql_file("160_update_award_subaward_fields.sql", where=where)
        self._execute_sql_file("170_mark_imported.sql")

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
            where = SQL("where {}").format(SQL(" or ").join(wheres))

            # Because our where clause is embedded in a dblink, we need to
            # wrap it in a literal again to get everything escaped properly.
            where = Literal(convert_composable_query_to_string(where))
            where = convert_composable_query_to_string(where)

            # Remove outer quotes
            return where[1:-1]

        return ""
