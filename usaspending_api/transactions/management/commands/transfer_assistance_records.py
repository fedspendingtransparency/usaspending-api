import logging
import psycopg2

from datetime import datetime, timezone
from django.core.management.base import BaseCommand
from pathlib import Path
from psycopg2.sql import Identifier, SQL

from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date
from usaspending_api.common.etl import ETLDBLinkTable, ETLTable, primatives
from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type
from usaspending_api.common.helpers.sql_helpers import get_broker_dsn_string, execute_dml_sql
from usaspending_api.common.helpers.timing_helpers import Timer
from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri, SCHEMA_HELP_TEXT
from usaspending_api.transactions.models import SourceAssistanceTransaction


CHUNK_SIZE = 15000
SUBMISSION_LOOKBACK_MINUTES = 15
LOGGER = logging.getLogger("console")


def store_broker_ids_in_file(id_iterable, file_name_prefix="transaction_load"):
    total_ids = 0
    file_path = Path("{}_{}".format(file_name_prefix, datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")))
    with open(str(file_path), "w") as f:
        for broker_id in id_iterable:
            total_ids += 1
            f.writelines("{}\n".format(broker_id))

    return file_path.resolve(), total_ids


def read_file_for_database_ids(provided_uri):
    """wrapped generator to read file and stream IDs"""
    try:
        with RetrieveFileFromUri(provided_uri).get_file_object() as file:
            while True:
                lines = file.readlines(
                    CHUNK_SIZE * 9
                )  # since this is by bytes, this allows a rough translation to lines
                lines = [line.decode("utf-8") for line in lines]
                if len(lines) == 0:
                    break
                yield lines

    except Exception as e:
        raise RuntimeError("Issue with reading/parsing file: {}".format(e))


def filepath_command_line_argument_type():
    """helper function for parsing files provided by the user"""

    def _filepath_command_line_argument_type(provided_uri):
        """"""
        try:
            with RetrieveFileFromUri(provided_uri).get_file_object() as file:
                while True:
                    lines = [line.decode("utf-8") for line in file.readlines(CHUNK_SIZE)]
                    if len(lines) == 0:
                        break
                    for line in lines:
                        yield int(line)

        except Exception as e:
            raise RuntimeError("Issue with reading/parsing file: {}".format(e))

    return _filepath_command_line_argument_type


class Command(BaseCommand):

    help = "Upsert assistance transactions from a broker system into USAspending"
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
            type=filepath_command_line_argument_type(),
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

    # def exit_signal_handler(signum, frame):
    # """ Attempt to gracefully handle the exiting of the job as a result of receiving an exit signal."""
    #     signal_or_human = BSD_SIGNALS.get(signum, signum)
    #     print("Received signal {}. Attempting to gracefully exit".format(signal_or_human))

    def handle(self, *args, **options):
        LOGGER.info("starting transfer script")
        self.start_time = datetime.now(timezone.utc)

        if options["incremental_date"]:
            LOGGER.info("INCREMENTAL LOAD")
            self.is_incremental = True
            options["datetime"] = get_last_load_date(self.last_load_record, SUBMISSION_LOOKBACK_MINUTES)
            if not options["datetime"]:
                raise SystemExit("No datetime stored in the database, unable to use --since-last-load")

        with Timer("Running script"):
            try:
                self.process(options)
            except (Exception, SystemExit, KeyboardInterrupt):
                LOGGER.exception("Fatal error")
                self.is_incremental = False  # disables update to the database last laoad date
            finally:
                self.cleanup()

    def process(self, options):
        with Timer("Compiling IDs to process"):
            self.file_path, self.total_ids_to_process = self.compile_transactions_to_process(options)

        LOGGER.info("{} IDs stored".format(self.total_ids_to_process))
        with Timer("Upsert records into destination database"):
            copy_broker_table_data(
                str(self.file_path),
                "published_award_financial_assistance",
                SourceAssistanceTransaction().table_name,
                "published_award_financial_assistance_id",
            )

    def cleanup(self):
        """Finalize the execution and cleanup for the next script run"""
        if self.is_incremental:
            update_last_load_date(self.last_load_record, self.start_time)
        # If the script fails beforre the file is created, don't raise exception
        # If the file was created and still exists, remove
        if hasattr(self, "file_path") and self.file_path.exists():
            self.file_path.unlink()

    def compile_transactions_to_process(self, cli_args):
        ids = []
        if cli_args["file"]:
            ids = cli_args["file"]
        elif cli_args["ids"]:
            ids = cli_args["ids"]
        else:
            ids = self.generate_ids_from_broker(cli_args)

        return store_broker_ids_in_file(ids, "assistance_load_ids")

    def parse_options(self, options):
        """Create the SQL predicate to limit which transaction records are transfered"""
        if options["reload_all"]:
            return ""
        elif options["datetime"]:
            return "AND updated_at >= '{}'".format(options["datetime"])
        else:
            ids = options["file"] if options["file"] else options["ids"]
            return "AND published_award_financial_assistance_id IN {}".format(tuple(ids))

    def generate_ids_from_broker(self, cli_args):
        sql = """
        select  published_award_financial_assistance_id
        from    published_award_financial_assistance
        where   is_active is true
        """
        sql += self.parse_options(cli_args)
        with psycopg2.connect(dsn=get_broker_dsn_string()) as connection:
            with connection.cursor("fabs_data_transfer") as cursor:
                cursor.execute(sql)
                while True:
                    id_list = [id[0] for id in cursor.fetchmany(size=CHUNK_SIZE)]
                    if not id_list:
                        break
                    for broker_id in id_list:
                        yield broker_id


def copy_broker_table_data(file_name, source_table, dest_table, primary_key):
    """Use Kirk's schmancy ETLTables"""
    destination_table = ETLTable(dest_table)
    source_table = ETLDBLinkTable(source_table, "broker_server", destination_table.data_types)
    insertable_columns = [c for c in destination_table.columns if c in source_table.columns]
    excluded = SQL(", ").join(
        [
            SQL("{dest} = {source}").format(dest=Identifier(field), source=SQL("EXCLUDED.") + Identifier(field))
            for field in destination_table.columns
        ]
    )

    upsert_sql_template = """
        INSERT INTO {destination_object_representation} ({insert_columns})
        SELECT      {select_columns}
        FROM        {source_object} AS {alias}
        ON CONFLICT ({primary_key}) DO UPDATE SET
        {excluded}
        RETURNING {primary_key}
    """
    alias = "s"

    for id_list in read_file_for_database_ids(file_name):
        int_id_tuple = tuple([int(x) for x in id_list])
        source_predicate_obj = [{"field": primary_key, "op": "IN", "values": tuple(int_id_tuple)}]

        sql = SQL(upsert_sql_template).format(
            primary_key=primary_key,
            alias=alias,
            destination_object_representation=destination_table.object_representation,
            insert_columns=primatives.make_column_list(insertable_columns),
            select_columns=primatives.make_column_list(insertable_columns, alias, destination_table.insert_overrides),
            source_object=source_table.complex_object_representation(source_predicate_obj),
            excluded=excluded,
        )
        with Timer("insert"):
            record_count = execute_dml_sql(sql)
        LOGGER.info("Success on {:,} upserts".format(record_count))
