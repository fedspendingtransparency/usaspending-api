import logging
import psycopg2

from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date
from usaspending_api.common.etl import ETLDBLinkTable, ETLTable, operations
from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type
from usaspending_api.common.helpers.sql_helpers import get_broker_dsn_string
from usaspending_api.common.helpers.timing_helpers import Timer
from usaspending_api.common.retrieve_file_from_uri import SCHEMA_HELP_TEXT
from usaspending_api.transactions.loader_functions import filepath_command_line_argument_type
from usaspending_api.transactions.loader_functions import read_file_for_database_ids
from usaspending_api.transactions.loader_functions import store_ids_in_file

CHUNK_SIZE = 25000
logger = logging.getLogger("script")


class BaseTransferClass:
    is_incremental = False
    successful_run = False
    upsert_records = 0

    def add_arguments(self, parser):
        mutually_exclusive_group = parser.add_mutually_exclusive_group(required=True)

        mutually_exclusive_group.add_argument(
            "--ids",
            nargs="+",
            type=int,
            help="Load/Reload transactions using this {} list (space-separated)".format(self.shared_pk),
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
            help="Equivalent to loading from date, but date is drawn from last update date recorded in DB",
        )
        mutually_exclusive_group.add_argument(
            "--file",
            dest="file",
            type=filepath_command_line_argument_type(chunk_count=1),
            help=(
                "Load/Reload transactions using {} values stored at this file path"
                " (one ID per line) {}".format(self.shared_pk, SCHEMA_HELP_TEXT)
            ),
        )
        mutually_exclusive_group.add_argument(
            "--reload-all",
            action="store_true",
            help=(
                "Script will load or reload all {} records from broker database, from all time."
                " This does NOT clear the USASpending database first".format(self.broker_source_table_name)
            ),
        )

    def handle(self, *args, **options):
        logger.info("STARTING SCRIPT")
        self.start_time = datetime.now(timezone.utc)
        self.options = options

        if self.options["incremental_date"]:
            self.is_incremental = True
            self.options["datetime"] = self.obtain_last_date()

        try:
            with Timer(message="script process", success_logger=logger.info, failure_logger=logger.error):
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
        with Timer(message="Compiling IDs to process", success_logger=logger.info, failure_logger=logger.error):
            self.file_path, self.total_ids_to_process = self.compile_transactions_to_process()

        logger.info("{:,} IDs stored".format(self.total_ids_to_process))
        with Timer(message="Upsert operation", success_logger=logger.info, failure_logger=logger.error):
            self.copy_broker_table_data(
                self.broker_source_table_name, self.destination_table_name, self.shared_pk,
            )

    def cleanup(self) -> None:
        """Finalize the execution and cleanup for the next script run"""
        logger.info("Processed {:,} transction records (insert/update)".format(self.upsert_records))
        if self.successful_run and (self.is_incremental or self.options["reload_all"]):
            logger.info("Updated last run time for next incremental load")
            update_last_load_date(self.last_load_record, self.start_time)

        if hasattr(self, "file_path") and self.file_path.exists():
            # If the script fails before the file is created, skip
            # If the file still exists, remove
            self.file_path.unlink()

        if self.successful_run:
            logger.info("Success. Completing execution")
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

        file_name = "{}_{}".format(self.working_file_prefix, self.start_time.strftime("%Y%m%d_%H%M%S_%f"))
        return store_ids_in_file(ids, file_name)

    def parse_options(self):
        """Create the SQL predicate to limit which transaction records are transfered"""
        if self.options["reload_all"]:
            logger.info("FULL RELOAD")
            return ""
        elif self.options["datetime"]:
            logger.info("Using datetime '{}'".format(self.options["datetime"]))
            return "updated_at >= '{}'".format(self.options["datetime"])

    def generate_ids_from_broker(self):
        sql = self.combine_sql()
        with psycopg2.connect(dsn=get_broker_dsn_string()) as connection:
            with connection.cursor("usaspending_data_transfer") as cursor:
                cursor.execute(sql.strip("\n"))
                while True:
                    id_list = [id[0] for id in cursor.fetchmany(size=CHUNK_SIZE)]
                    if not id_list:
                        break
                    for broker_id in id_list:
                        yield broker_id

    def combine_sql(self):
        sql = self.broker_select_sql.format(self.shared_pk, self.broker_source_table_name)
        predicate = self.parse_options()

        keyword = "WHERE"
        if "WHERE" in sql:
            keyword = "AND"

        return "{} {} {}".format(sql, keyword, predicate)

    def copy_broker_table_data(self, source_tablename, dest_tablename, primary_key):
        """Loop through the batches of IDs and load using the ETL tables"""
        destination = ETLTable(dest_tablename)
        source = ETLDBLinkTable(source_tablename, "broker_server", destination.data_types)

        for id_list in read_file_for_database_ids(str(self.file_path), CHUNK_SIZE):
            predicate = [{"field": primary_key, "op": "IN", "values": tuple(id_list)}]
            with Timer(message="upsert", success_logger=logger.info, failure_logger=logger.error):
                record_count = operations.upsert_records_with_predicate(source, destination, predicate, primary_key)
            logger.info("Success on {:,} upserts".format(record_count))
            self.upsert_records += record_count


__all__ = ["BaseTransferClass"]
