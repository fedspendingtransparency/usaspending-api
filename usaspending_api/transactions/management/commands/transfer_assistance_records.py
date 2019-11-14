import logging
import psycopg2

from datetime import datetime, timezone
from django.core.management.base import BaseCommand
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
from usaspending_api.transactions.models import SourceAssistanceTransaction

CHUNK_SIZE = 25000
SUBMISSION_LOOKBACK_MINUTES = 15
logger = logging.getLogger("script")


class Command(BaseCommand):
    help = "Upsert assistance transactions from a broker database into an USAspending database"
    last_load_record = "source_assistance_transaction"
    is_incremental = False

    def add_arguments(self, parser):
        mutually_exclusive_group = parser.add_mutually_exclusive_group(required=True)

        mutually_exclusive_group.add_argument(
            "--ids",
            nargs="+",
            type=int,
            help="Load/Reload transactions using this published_award_financial_assistance_id list (space-separated)",
        )
        mutually_exclusive_group.add_argument(
            "--date",
            dest="datetime",
            type=datetime_command_line_argument_type(naive=True),  # Broker date/times are naive.
            help="Load/Reload all FPDS records from the provided datetime to the script execution start time.",
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
                "Load/Reload transactions using published_award_financial_assistance_id values stored at this file path"
                " (one ID per line) {}".format(SCHEMA_HELP_TEXT)
            ),
        )
        mutually_exclusive_group.add_argument(
            "--reload-all",
            action="store_true",
            help=(
                "Script will load or reload all FABS records in broker database, from all time."
                " This does NOT clear the USASpending database first"
            ),
        )

    def handle(self, *args, **options):
        logger.info("starting transfer script")
        self.start_time = datetime.now(timezone.utc)
        self.options = options

        if self.options["incremental_date"]:
            logger.info("INCREMENTAL LOAD")
            self.is_incremental = True
            self.options["datetime"] = self.obtain_last_date()
        elif self.options["reload_all"]:
            self.is_incremental = True

        with Timer(message="script process", success_logger=logger.info, failure_logger=logger.error):
            try:
                self.process()
            except (Exception, SystemExit, KeyboardInterrupt):
                logger.exception("Fatal error")
                self.is_incremental = False  # disables update to the database last laoad date
            finally:
                self.cleanup()

    def obtain_last_date(self):
        dt = get_last_load_date(self.last_load_record, SUBMISSION_LOOKBACK_MINUTES)
        if not dt:
            raise SystemExit("No datetime stored in the database, unable to use --since-last-load")
        return dt

    def process(self) -> None:
        with Timer(message="Compiling IDs to process", success_logger=logger.info, failure_logger=logger.error):
            self.file_path, self.total_ids_to_process = self.compile_transactions_to_process()

        logger.info("{} IDs stored".format(self.total_ids_to_process))
        with Timer(message="Upsert operation", success_logger=logger.info, failure_logger=logger.error):
            copy_broker_table_data(
                str(self.file_path),
                "published_award_financial_assistance",
                SourceAssistanceTransaction().table_name,
                "published_award_financial_assistance_id",
            )

    def cleanup(self) -> None:
        """Finalize the execution and cleanup for the next script run"""
        if self.is_incremental:
            logger.info("Updated last run time for next incremental load")
            update_last_load_date(self.last_load_record, self.start_time)
        # If the script fails beforre the file is created, don't raise an exception
        # If the file was created and still exists, remove
        if hasattr(self, "file_path") and self.file_path.exists():
            self.file_path.unlink()

    def compile_transactions_to_process(self) -> Tuple[Path, int]:
        ids = []
        if self.options["file"]:
            ids = self.options["file"]
        elif self.options["ids"]:
            ids = self.options["ids"]
        else:
            ids = self.generate_ids_from_broker()

        file_name = "assistance_load_ids_{}".format(self.start_time.strftime("%Y%m%d_%H%M%S_%f"))
        return store_ids_in_file(ids, file_name)

    def parse_options(self):
        """Create the SQL predicate to limit which transaction records are transfered"""
        if self.options["reload_all"]:
            return ""
        elif self.options["datetime"]:
            return "AND updated_at >= '{}'".format(self.options["datetime"])
        else:
            ids = self.options["file"] if self.options["file"] else self.options["ids"]
            return "AND published_award_financial_assistance_id IN {}".format(tuple(ids))

    def generate_ids_from_broker(self):
        sql = """
        SELECT  published_award_financial_assistance_id
        FROM    published_award_financial_assistance
        WHERE   is_active IS true
        """.strip(
            "\n"
        )
        sql += self.parse_options()
        with psycopg2.connect(dsn=get_broker_dsn_string()) as connection:
            with connection.cursor("fabs_data_transfer") as cursor:
                cursor.execute(sql)
                while True:
                    id_list = [id[0] for id in cursor.fetchmany(size=CHUNK_SIZE)]
                    if not id_list:
                        break
                    for broker_id in id_list:
                        yield broker_id


def copy_broker_table_data(file_name, source_tablename, dest_tablename, primary_key):
    """Loop through the batches of IDs and load using the ETL tables"""
    destination = ETLTable(dest_tablename)
    source = ETLDBLinkTable(source_tablename, "broker_server", destination.data_types)

    for id_list in read_file_for_database_ids(file_name, CHUNK_SIZE):
        predicate = [{"field": primary_key, "op": "IN", "values": tuple(id_list)}]
        with Timer(message="upsert", success_logger=logger.info, failure_logger=logger.error):
            record_count = operations.upsert_records_with_predicate(source, destination, predicate, primary_key)
        logger.info("Success on {:,} upserts".format(record_count))
