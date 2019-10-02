import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from pathlib import Path
from usaspending_api.common.etl import ETLTable, operations, mixins
from usaspending_api.common.helpers.timing_helpers import Timer


logger = logging.getLogger("console")


class Command(mixins.ETLMixin, BaseCommand):

    help = "Load subawards from Broker into USAspending."

    full_reload = False

    etl_logger_function = logger.info
    etl_dml_sql_directory = Path(__file__).resolve().parent / "load_subawards_sql"

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
            self._execute_dml_sql("delete from broker_subaward", "Empty broker_subaward table")
            self._execute_dml_sql("delete from subaward", "Empty subaward table")

        self._execute_function_log_rows_affected(
            operations.stage_dblink_table,
            "Copy Broker subaward table",
            source=remote_subaward,
            destination=broker_subaward,
            staging=temp_broker_subaward,
        )

        # Create a list of new or updated subawards that we can use to filter down
        # subsequent operations.
        self._execute_function_log_rows_affected(
            operations.identify_new_or_updated,
            "Identify new/updated rows",
            source=temp_broker_subaward,
            destination=broker_subaward,
            staging=temp_new_or_updated,
        )

        self._execute_function_log_rows_affected(
            operations.delete_obsolete_rows,
            "Delete obsolete broker_subaward rows",
            source=temp_broker_subaward,
            destination=broker_subaward,
        )

        self._execute_function_log_rows_affected(
            operations.update_changed_rows,
            "Update changed broker_subaward rows",
            source=temp_broker_subaward,
            destination=broker_subaward,
        )

        self._execute_function_log_rows_affected(
            operations.insert_missing_rows,
            "Insert missing broker_subaward rows",
            source=temp_broker_subaward,
            destination=broker_subaward,
        )

        # The Broker subaward table takes up a good bit of space so let's explicitly free
        # it up before continuing.
        self._execute_dml_sql("drop table if exists temp_load_subawards_broker_subaward", "Drop staging table")

        self._execute_function_log_rows_affected(
            operations.delete_obsolete_rows,
            "Delete obsolete subaward rows",
            source=broker_subaward,
            destination=subaward,
        )

        self._execute_etl_dml_sql_directory_file("030_frame_out_subawards")
        self._execute_etl_dml_sql_directory_file("040_enhance_with_awards_data")
        self._execute_etl_dml_sql_directory_file("050_enhance_with_transaction_data")
        self._execute_etl_dml_sql_directory_file("060_enhance_with_cfda_data")
        self._execute_etl_dml_sql_directory_file("070_enhance_with_awarding_agency")
        self._execute_etl_dml_sql_directory_file("080_enhance_with_funding_agency")
        self._execute_etl_dml_sql_directory_file("090_create_temp_address_table")
        self._execute_etl_dml_sql_directory_file("100_enhance_with_pop_county_city")
        self._execute_etl_dml_sql_directory_file("110_enhance_with_pop_country")
        self._execute_etl_dml_sql_directory_file("120_enhance_with_recipient_location_county_city")
        self._execute_etl_dml_sql_directory_file("130_enhance_with_recipient_location_country")

        self._execute_function_log_rows_affected(
            operations.update_changed_rows, "Update changed subaward rows", source=temp_subaward, destination=subaward
        )
        self._execute_function_log_rows_affected(
            operations.insert_missing_rows, "Insert missing subaward rows", source=temp_subaward, destination=subaward
        )

        self._execute_etl_dml_sql_directory_file("140_link_awards")
        self._execute_etl_dml_sql_directory_file("150_update_awards")

        # Not strictly necessary for temporary tables, but drop them just to be thorough.
        self._execute_dml_sql("drop table if exists temp_load_subawards_new_or_updated", "Drop new/updated temp table")
        self._execute_dml_sql("drop table if exists temp_load_subawards_subaward", "Drop subaward temp table")
