import logging
import psycopg2

from datetime import datetime, timezone
from django.conf import settings
from django.core.management import call_command
from pathlib import Path
from typing import Tuple

from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date
from usaspending_api.common.etl.postgres import ETLDBLinkTable, ETLTable
from usaspending_api.common.etl.postgres import operations
from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type
from usaspending_api.common.helpers.sql_helpers import get_broker_dsn_string
from usaspending_api.common.helpers.timing_helpers import ScriptTimer as Timer
from usaspending_api.common.retrieve_file_from_uri import SCHEMA_HELP_TEXT
from usaspending_api.transactions.loader_functions import filepath_command_line_argument_type
from usaspending_api.transactions.loader_functions import read_file_for_database_ids
from usaspending_api.transactions.loader_functions import store_ids_in_file

logger = logging.getLogger("script")


class AgnosticTransactionLoader:
    beginning_of_time = "1970-01-01"
    chunk_size = 25000
    is_incremental = False
    successful_run = False
    upsert_records = 0

    def add_arguments(self, parser):
        mutually_exclusive_group = parser.add_mutually_exclusive_group(required=True)

        mutually_exclusive_group.add_argument(
            "--ids", nargs="+", help=f"Load/Reload transactions using this {self.shared_pk} list (space-separated)"
        )
        mutually_exclusive_group.add_argument(
            "--date",
            dest="datetime",
            type=datetime_command_line_argument_type(naive=True),  # Broker date/times are naive.
            help="Load/Reload records from the provided datetime to the script execution start time.",
        )
        mutually_exclusive_group.add_argument(
            "--since-last-load",
            dest="incremental_date",
            action="store_true",
            help="Equivalent to loading from date, but date is drawn from last update date recorded in DB.",
        )
        mutually_exclusive_group.add_argument(
            "--file",
            dest="file",
            type=filepath_command_line_argument_type(chunk_count=self.chunk_size),
            help=(
                f"Load/Reload transactions using {self.shared_pk} values stored at this file path"
                f" (one ID per line) {SCHEMA_HELP_TEXT}"
            ),
        )
        mutually_exclusive_group.add_argument(
            "--reload-all",
            action="store_true",
            help=(
                f"Script will load or reload all {self.broker_source_table_name} records from broker database,"
                " from all time. This does NOT clear the USASpending database first."
            ),
        )
        parser.add_argument(
            "--process-deletes",
            action="store_true",
            help=(
                "If not in local mode, process deletes before beginning the upsert operations."
                " This shouldn't be used with --file or --ids parameters"
            ),
        )

    def handle(self, *args, **options):
        with Timer(message="Script"):
            self.run_script(*args, **options)

    def run_script(self, *args, **options):
        self.start_time = datetime.now(timezone.utc)
        self.options = options

        if self.options["incremental_date"]:
            self.is_incremental = True
            self.options["datetime"] = self.obtain_last_date()

        if self.options["process_deletes"]:
            delete_date = self.options["datetime"]
            if not delete_date:
                delete_date = self.beginning_of_time

            with Timer(message="Processing deletes"):
                delete_job_status = call_command(self.delete_management_command, f"--date={delete_date}")

            if delete_job_status != 0:
                raise RuntimeError("Fatal error. Problem with the deletes")

        try:
            with Timer(message="Load Process"):
                self.process()
            self.successful_run = True
        except (Exception, SystemExit, KeyboardInterrupt):
            logger.exception("Fatal error")
        finally:
            self.cleanup()

    def obtain_last_date(self):
        dt = get_last_load_date(self.last_load_record, self.lookback_minutes)
        if not dt:
            raise SystemExit("No datetime stored in the database, unable to use --since-last-load")
        return dt

    def process(self) -> None:
        with Timer(message="Compiling IDs to process"):
            self.file_path, self.total_ids_to_process = self.compile_transactions_to_process()

        logger.info(f"{self.total_ids_to_process:,} IDs stored")
        with Timer(message="Transferring Data"):
            self.copy_broker_table_data(self.broker_source_table_name, self.destination_table_name, self.shared_pk)

    def cleanup(self) -> None:
        """Finalize the execution and cleanup for the next script run"""
        logger.info(f"Processed {self.upsert_records:,} transaction records (insert/update)")
        if self.successful_run and (self.is_incremental or self.options["reload_all"]):
            logger.info("Updated last run time for next incremental load")
            update_last_load_date(self.last_load_record, self.start_time)

        if hasattr(self, "file_path") and self.file_path.exists():
            # If the script fails before the file is created, skip
            # If the file still exists, remove
            self.file_path.unlink()

        if self.successful_run:
            logger.info(f"Loading {self.destination_table_name} completed successfully")
        else:
            logger.info("Failed state on exit")
            raise SystemExit(1)

    def compile_transactions_to_process(self) -> Tuple[Path, int]:
        ids = []
        if self.options["file"]:
            ids = self.options["file"]
            logger.info("using provided IDs in file")
        elif self.options["ids"]:
            ids = self.options["ids"]
            logger.info("using provided IDs")
        else:
            ids = self.generate_ids_from_broker()

        file_name = f"{self.working_file_prefix}_{self.start_time.strftime('%Y%m%d_%H%M%S_%f')}"
        return store_ids_in_file(ids, file_name, is_numeric=False)

    def generate_ids_from_broker(self):
        sql = self.combine_sql()

        with psycopg2.connect(dsn=get_broker_dsn_string()) as connection:
            with connection.cursor("usaspending_data_transfer") as cursor:
                cursor.execute(sql.strip("\n"))
                while True:
                    id_list = [id[0] for id in cursor.fetchmany(size=self.chunk_size)]
                    if not id_list:
                        break
                    for broker_id in id_list:
                        yield broker_id

    def combine_sql(self):
        """Create SQL used to fetch transaction ids for records marked to transfer"""
        if self.options["reload_all"]:
            logger.info("FULL RELOAD")
            sql = self.broker_full_select_sql
            optional_predicate = ""
        elif self.options["datetime"]:
            logger.info(f"Using datetime '{self.options['datetime']}'")
            sql = self.broker_incremental_select_sql
            predicate = f"\"updated_at\" >= '{self.options['datetime']}'"

            if "where" in sql.lower():
                optional_predicate = f"and {predicate}"
            else:
                optional_predicate = f"where {predicate}"

        return sql.format(id=self.shared_pk, table=self.broker_source_table_name, optional_predicate=optional_predicate)

    def copy_broker_table_data(self, source_tablename, dest_tablename, primary_key):
        """Loop through the batches of IDs and load using the ETL tables"""
        destination = ETLTable(dest_tablename, schema_name="raw")
        source = ETLDBLinkTable(source_tablename, settings.DATA_BROKER_DBLINK_NAME, destination.data_types)
        transactions_remaining_count = self.total_ids_to_process

        for id_list in read_file_for_database_ids(str(self.file_path), self.chunk_size, is_numeric=False):
            with Timer(message=f"Upsert {len(id_list):,} records"):
                if len(id_list) != 0:
                    predicate = self.extra_predicate + [{"field": primary_key, "op": "IN", "values": tuple(id_list)}]
                    record_count = operations.upsert_records_with_predicate(
                        source, destination, predicate, primary_key, self.is_case_insensitive_pk_match
                    )
                else:
                    logger.warning("No records to load. Please check parameters and settings to confirm accuracy")
                    record_count = 0

            if transactions_remaining_count > len(id_list):
                transactions_remaining_count -= len(id_list)
            else:
                transactions_remaining_count = 0
            self.upsert_records += record_count
            percentage = self.upsert_records * 100 / self.total_ids_to_process if self.total_ids_to_process != 0 else 0
            logger.info(
                f"{self.upsert_records:,} successful upserts, "
                f"{transactions_remaining_count:,} remaining. "
                f"[{percentage:.2f}%]"
            )
