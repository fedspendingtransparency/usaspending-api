import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from pathlib import Path
from usaspending_api.common.etl.postgres import ETLDBLinkTable, ETLTable, ETLTemporaryTable
from usaspending_api.common.etl.postgres import mixins, operations
from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer
from usaspending_api.etl.operations.subaward.update_city_county import update_subaward_city_county


logger = logging.getLogger("script")


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
        logger.info("FULL RELOAD SWITCH: {}".format(self.full_reload))

        with Timer("Load subawards"):
            try:
                with transaction.atomic():
                    self._perform_load()
                    t = Timer("Commit subaward transaction")
                    t.log_starting_message()
                t.log_success_message()
            except Exception:
                logger.error("ALL CHANGES ROLLED BACK DUE TO EXCEPTION")
                raise

    def _perform_load(self):
        """ Grab the Broker subaward table and use it to update ours. """

        broker_subaward = ETLTable("broker_subaward", schema_name="raw")
        subaward = ETLTable("subaward", schema_name="int")

        remote_subaward = ETLDBLinkTable("subaward", settings.DATA_BROKER_DBLINK_NAME, broker_subaward.data_types)

        temp_broker_subaward = ETLTemporaryTable("temp_load_subawards_broker_subaward")
        temp_new_or_updated = ETLTemporaryTable("temp_load_subawards_new_or_updated")
        temp_subaward = ETLTemporaryTable("temp_load_subawards_subaward")

        if self.full_reload:
            logger.info("--full-reload switch provided.  Emptying subaward tables.")
            self._execute_dml_sql("delete from broker_subaward", "Empty broker_subaward table")
            self._execute_dml_sql("delete from subaward", "Empty subaward table")

        self._execute_function_and_log(
            operations.stage_table,
            "Copy Broker subaward table",
            source=remote_subaward,
            destination=broker_subaward,
            staging=temp_broker_subaward,
        )

        # To help performance.
        self._execute_dml_sql(
            "create index idx_temp_load_subawards_broker_subaward on temp_load_subawards_broker_subaward(id)",
            "Create temporary index",
        )

        # Create a list of new or updated subawards that we can use to filter down
        # subsequent operations.
        self._execute_function_and_log(
            operations.identify_new_or_updated,
            "Identify new/updated rows",
            source=temp_broker_subaward,
            destination=broker_subaward,
            staging=temp_new_or_updated,
        )

        self._delete_update_insert_rows("broker_subawards", temp_broker_subaward, broker_subaward)

        # The Broker subaward table takes up a good bit of space so let's explicitly free
        # it up before continuing.
        self._execute_dml_sql("drop table if exists temp_load_subawards_broker_subaward", "Drop staging table")

        self._execute_etl_dml_sql_directory_file("030_frame_out_subawards")
        self._execute_etl_dml_sql_directory_file("040_enhance_with_awards_data")
        self._execute_etl_dml_sql_directory_file("050_enhance_with_transaction_fpds_data.sql")
        self._execute_etl_dml_sql_directory_file("051_enhance_with_transaction_fabs_data.sql")
        self._execute_etl_dml_sql_directory_file("060_enhance_with_cfda_data")
        self._execute_etl_dml_sql_directory_file("070_enhance_with_awarding_agency")
        self._execute_etl_dml_sql_directory_file("080_enhance_with_funding_agency")

        with Timer("Enhance with city and county information"):
            update_subaward_city_county("temp_load_subawards_subaward")

        self._execute_etl_dml_sql_directory_file("110_enhance_with_pop_country")
        self._execute_etl_dml_sql_directory_file("130_enhance_with_recipient_location_country")

        # Delete from broker_subaward because temp_subaward only has new/updated rows.
        self._execute_function_and_log(
            operations.delete_obsolete_rows, "Delete obsolete subawards", broker_subaward, subaward
        )

        # But update and insert from temp_subaward.
        self._execute_function_and_log(
            operations.update_changed_rows, "Update changed subawards", temp_subaward, subaward
        )
        self._execute_function_and_log(
            operations.insert_missing_rows, "Insert missing subawards", temp_subaward, subaward
        )

        self._execute_etl_dml_sql_directory_file("140_link_awards")
        self._execute_etl_dml_sql_directory_file("150_update_awards")

        # This is for unit tests.  Temporary tables go away on their own once the transaction/session
        # drops, but because unit tests run in a transaction, our temporary tables do not go away
        # between loads... which is problematic.
        self._execute_dml_sql("drop table if exists temp_load_subawards_new_or_updated", "Drop new/updated temp table")
        self._execute_dml_sql("drop table if exists temp_load_subawards_subaward", "Drop subaward temp table")
