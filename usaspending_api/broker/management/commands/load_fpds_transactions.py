import logging
import psycopg2
import re

from datetime import datetime, timezone
from django.core.management.base import BaseCommand
from typing import IO, List, AnyStr, Optional

from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date
from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type
from usaspending_api.common.helpers.etl_helpers import update_c_to_d_linkages
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri
from usaspending_api.etl.award_helpers import update_awards, update_procurement_awards, prune_empty_awards
from usaspending_api.etl.transaction_loaders.fpds_loader import load_fpds_transactions, failed_ids, delete_stale_fpds
from usaspending_api.transactions.transaction_delete_journal_helpers import retrieve_deleted_fpds_transactions

logger = logging.getLogger("script")

CHUNK_SIZE = 15000

ALL_FPDS_QUERY = "SELECT {} FROM source_procurement_transaction"


class Command(BaseCommand):
    help = "Sync USAspending DB FPDS data using source transaction for new or modified records and S3 for deleted IDs"

    modified_award_ids = []

    @staticmethod
    def get_cursor_for_date_query(connection, date, count=False):
        if count:
            db_cursor = connection.cursor()
            db_query = ALL_FPDS_QUERY.format("COUNT(*)")
        else:
            db_cursor = connection.cursor("fpds_load", cursor_factory=psycopg2.extras.DictCursor)
            db_query = ALL_FPDS_QUERY.format("detached_award_procurement_id")

        if date:
            db_cursor.execute(db_query + " WHERE updated_at >= %s", [date])
        else:
            db_cursor.execute(db_query)
        return db_cursor

    def load_fpds_incrementally(self, date: Optional[datetime], chunk_size: int = CHUNK_SIZE) -> None:
        """Process incremental loads based on a date range or full data loads"""

        if date is None:
            logger.info("Skipping deletes. Fetching all fpds transactions...")
        else:
            logger.info(f"Handling fpds transactions since {date}...")

            detached_award_procurement_ids = retrieve_deleted_fpds_transactions(start_datetime=date)
            stale_awards = delete_stale_fpds(detached_award_procurement_ids)
            self.modified_award_ids.extend(stale_awards)

        with psycopg2.connect(dsn=get_database_dsn_string()) as connection:
            logger.info("Fetching records to update")
            total_records = self.get_cursor_for_date_query(connection, date, True).fetchall()[0][0]
            records_processed = 0
            logger.info("{} total records to update".format(total_records))
            cursor = self.get_cursor_for_date_query(connection, date)
            while True:
                id_list = cursor.fetchmany(chunk_size)
                if len(id_list) == 0:
                    break
                logger.info("Loading batch (size: {}) from date query...".format(len(id_list)))
                self.modified_award_ids.extend(load_fpds_transactions([row[0] for row in id_list]))
                records_processed = records_processed + len(id_list)
                logger.info("{} out of {} processed".format(records_processed, total_records))

    @staticmethod
    def gen_read_file_for_ids(file: IO[AnyStr], chunk_size: int = CHUNK_SIZE) -> List[str]:
        """ """
        while True:
            lines = [line for line in (file.readline().decode("utf-8").strip() for _ in range(chunk_size)) if line]
            yield lines

            if len(lines) < chunk_size:
                break

    def load_fpds_from_file(self, file_path: str) -> None:
        """Loads arbitrary set of ids, WITHOUT checking for deletes"""
        total_count = 0
        with RetrieveFileFromUri(file_path).get_file_object() as file:
            logger.info(f"Loading transactions from IDs in {file_path}")
            for next_batch in self.gen_read_file_for_ids(file):
                id_list = [int(re.search(r"\d+", x).group()) for x in next_batch]
                total_count += len(id_list)
                logger.info(f"Loading next batch (size: {len(id_list)}, ids {id_list[0]}-{id_list[-1]})...")
                self.modified_award_ids.extend(load_fpds_transactions(id_list))

        logger.info(f"Total transaction IDs in file: {total_count}")

    @staticmethod
    def update_award_records(awards, skip_cd_linkage=True):
        if awards:
            unique_awards = set(awards)
            logger.info(f"{len(unique_awards)} award records impacted by transaction DML operations")
            logger.info(f"{prune_empty_awards(tuple(unique_awards))} award records removed")
            logger.info(f"{update_awards(tuple(unique_awards))} award records updated")
            logger.info(
                f"{update_procurement_awards(tuple(unique_awards))} award records updated on FPDS-specific fields"
            )
            if not skip_cd_linkage:
                update_c_to_d_linkages("contract")
        else:
            logger.info("No award records to update")

    def add_arguments(self, parser):
        mutually_exclusive_group = parser.add_mutually_exclusive_group(required=True)

        mutually_exclusive_group.add_argument(
            "--ids",
            nargs="+",
            type=int,
            help="Load/Reload transactions using this detached_award_procurement_id list (space-separated)",
        )
        mutually_exclusive_group.add_argument(
            "--date",
            dest="date",
            type=datetime_command_line_argument_type(naive=True),  # Broker date/times are naive.
            help="Load/Reload all FPDS records from the provided datetime to the script execution start time.",
        )
        mutually_exclusive_group.add_argument(
            "--since-last-load",
            action="store_true",
            help="Equivalent to loading from date, but date is drawn from last update date recorded in DB",
        )
        mutually_exclusive_group.add_argument(
            "--file",
            metavar="FILEPATH",
            type=str,
            help="Load/Reload transactions using the detached_award_procurement_id list stored at this file path (one ID per line)"
            "to reload, one ID per line. Nonexistent IDs will be ignored.",
        )
        mutually_exclusive_group.add_argument(
            "--reload-all",
            action="store_true",
            help="Script will load or reload all FPDS records in source tables, from all time. This does NOT clear the USAspending database first",
        )

    def handle(self, *args, **options):

        # Record script execution start time to update the FPDS last updated date in DB as appropriate
        update_time = datetime.now(timezone.utc)

        if options["reload_all"]:
            self.load_fpds_incrementally(None)

        elif options["date"]:
            self.load_fpds_incrementally(options["date"])

        elif options["ids"]:
            self.modified_award_ids.extend(load_fpds_transactions(options["ids"]))

        elif options["file"]:
            self.load_fpds_from_file(options["file"])

        elif options["since_last_load"]:
            last_load = get_last_load_date("fpds")
            if not last_load:
                raise ValueError("No last load date for FPDS stored in the database")
            self.load_fpds_incrementally(last_load)

        self.update_award_records(awards=self.modified_award_ids, skip_cd_linkage=False)

        logger.info(f"Script took {datetime.now(timezone.utc) - update_time}")

        if failed_ids:
            failed_id_str = ", ".join([str(id) for id in failed_ids])
            logger.error(f"The following detached_award_procurement_ids failed to load: [{failed_id_str}]")
            raise SystemExit(1)

        if options["reload_all"] or options["since_last_load"]:
            # we wait until after the load finishes to update the load date because if this crashes we'll need to load again
            update_last_load_date("fpds", update_time)

        logger.info(f"Successfully Completed")
