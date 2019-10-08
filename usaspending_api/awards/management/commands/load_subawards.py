import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from pathlib import Path
from usaspending_api.common.etl import ETLTable, operations
from usaspending_api.common.helpers.sql_helpers import execute_update_sql
from usaspending_api.common.helpers.timing_helpers import Timer


logger = logging.getLogger("console")


class Command(BaseCommand):

    help = "Load subawards from Broker into USAspending."
    full_reload = False
    sql_path = Path(__file__).resolve().parent / "load_subawards_sql"

    def add_arguments(self, parser):
        parser.add_argument(
            "--full-reload",
            action="store_true",
            default=self.full_reload,
            help="Empties the USAspending subaward and broker_subaward tables before loading.",
        )

    def handle(self, *args, **options):

        self.full_reload = options["full_reload"]

        with Timer("Load subawards"):
            try:
                with transaction.atomic():
                    self._perform_load()
                    t = Timer("Committing transaction")
                    t.log_starting_message()
                t.log_success_message()
            except Exception:
                logger.error("ALL CHANGES WERE ROLLED BACK DUE TO EXCEPTION")
                raise

    @staticmethod
    def _execute(message, function, *args, **kwargs):
        """ Execute an import step. """

        with Timer(message):
            results = function(*args, **kwargs)
            if type(results) is int and results > -1:
                logger.info("{:,} rows affected".format(results))
            return results

    def _execute_sql_file(self, filename):
        """ Read in a SQL file and execute it. """

        filepath = (self.sql_path / filename).with_suffix(".sql")
        return self._execute(filename, execute_update_sql, sql=filepath.read_text())

    def _perform_load(self):
        """ Grab the Broker subaward table and use it to update ours. """

        # All of the tables involved in this process.  Yikes.
        broker_subaward = ETLTable(table_name="broker_subaward", schema_name="public")
        remote_subaward = ETLTable(table_name="subaward", schema_name="public", dblink_name="broker_server")
        subaward = ETLTable(table_name="subaward", schema_name="public")
        temp_broker_subaward = ETLTable(table_name="temp_load_subawards_broker_subaward")
        temp_new_or_updated = ETLTable(table_name="temp_load_subawards_new_or_updated")
        temp_subaward = ETLTable(table_name="temp_load_subawards_subaward")

        if self.full_reload:
            logger.info("--full-reload switch provided.  Emptying subaward tables.")
            self._execute("Empty broker_subaward table", execute_update_sql, sql="delete from broker_subaward")
            self._execute("Empty subaward table", execute_update_sql, sql="delete from subaward")

        self._execute(
            "Copy Broker subaward table",
            operations.stage_dblink_table,
            source=remote_subaward,
            destination=broker_subaward,
            staging=temp_broker_subaward,
        )

        # Create a list of new or updated subawards that we can use to filter down
        # subsequent operations.
        self._execute(
            "Identify new or updated rows",
            operations.identify_new_or_updated,
            source=temp_broker_subaward,
            destination=broker_subaward,
            staging=temp_new_or_updated,
        )

        self._execute(
            "Delete obsolete broker_subaward rows",
            operations.delete_obsolete_rows,
            source=temp_broker_subaward,
            destination=broker_subaward,
        )
        self._execute(
            "Update changed broker_subaward rows",
            operations.update_changed_rows,
            source=temp_broker_subaward,
            destination=broker_subaward,
        )
        self._execute(
            "Insert missing broker_subaward rows",
            operations.insert_missing_rows,
            source=temp_broker_subaward,
            destination=broker_subaward,
        )

        # The Broker subaward table takes up a good bit of space so let's explicitly free
        # it up before continuing.
        self._execute(
            "Drop staging table", execute_update_sql, sql="drop table if exists temp_load_subawards_broker_subaward"
        )

        self._execute(
            "Delete obsolete subaward rows",
            operations.delete_obsolete_rows,
            source=broker_subaward,
            destination=subaward,
        )

        self._execute_sql_file("030_frame_out_subawards")
        self._execute_sql_file("040_enhance_with_awards_data")
        self._execute_sql_file("050_enhance_with_transaction_data")
        self._execute_sql_file("060_enhance_with_cfda_data")
        self._execute_sql_file("070_enhance_with_awarding_agency")
        self._execute_sql_file("080_enhance_with_funding_agency")
        self._execute_sql_file("090_create_temp_address_table")
        self._execute_sql_file("100_enhance_with_pop_county_city")
        self._execute_sql_file("110_enhance_with_pop_country")
        self._execute_sql_file("120_enhance_with_recipient_location_county_city")
        self._execute_sql_file("130_enhance_with_recipient_location_country")

        self._execute(
            "Update changed subaward rows", operations.update_changed_rows, source=temp_subaward, destination=subaward
        )
        self._execute(
            "Insert missing subaward rows", operations.insert_missing_rows, source=temp_subaward, destination=subaward
        )

        self._execute_sql_file("140_link_awards")
        self._execute_sql_file("150_update_awards")

        # Clean up the remaining temp tables.
        self._execute(
            "Drop new or updated temp table",
            execute_update_sql,
            sql="drop table if exists temp_load_subawards_new_or_updated",
        )
        self._execute(
            "Drop subaward temp table", execute_update_sql, sql="drop table if exists temp_load_subawards_subaward"
        )
