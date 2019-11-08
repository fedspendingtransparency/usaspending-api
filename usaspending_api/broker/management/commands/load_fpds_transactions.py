from django.core.management.base import BaseCommand
import logging
import re
import psycopg2
from datetime import datetime, timezone

from usaspending_api.etl.transaction_loaders.fpds_loader import load_ids, failed_ids, destroy_orphans, delete_stale_fpds
from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri
from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type
from usaspending_api.common.helpers.sql_helpers import get_broker_dsn_string
from usaspending_api.common.helpers.etl_helpers import update_c_to_d_linkages
from usaspending_api.etl.award_helpers import update_awards, update_contract_awards
from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date

logger = logging.getLogger("console")

CHUNK_SIZE = 5000

ALL_FPDS_QUERY = "SELECT {} FROM detached_award_procurement"


class Command(BaseCommand):
    help = "Sync USAspending DB FPDS data using Broker for new or modified records and S3 for deleted IDs"

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

    def load_fpds_incrementally(self, date):
        """
        Handles "increment" loads, which includes deleting transactions so long as a date is provided
        :param date: if provided, loader will load and delete all transactions starting from this date
        :return: nothing
        """
        if date is None:
            logger.info("fetching all fpds transactions...")
        else:
            logger.info("fetching fpds transactions since {}...".format(str(date)))

        # First clear any transactions marked for deletion. If the transactions have since been re-added, they will be back in the broker DB and will be re-inserted in the next step
        if date:
            stale_awards = delete_stale_fpds(date.date())
            self.modified_award_ids.extend(stale_awards)

        with psycopg2.connect(dsn=get_broker_dsn_string()) as connection:
            total_records = self.get_cursor_for_date_query(connection, date, True).fetchall()[0][0]
            records_processed = 0
            logger.info("{} total records".format(total_records))
            cursor = self.get_cursor_for_date_query(connection, date)
            while True:
                id_list = cursor.fetchmany(CHUNK_SIZE)
                if len(id_list) == 0:
                    break
                logger.info("Loading batch from date query (size: {})...".format(len(id_list)))
                self.modified_award_ids.extend(load_ids([row[0] for row in id_list]))
                records_processed = records_processed + len(id_list)
                logger.info("{} out of {} processed".format(records_processed, total_records))

    @staticmethod
    def next_file_batch_generator(file):
        while True:
            lines = file.readlines(CHUNK_SIZE * 8)  # since this is by bytes, this allows a rough translation to lines
            lines = [line.decode("utf-8") for line in lines]
            if len(lines) == 0:
                break
            yield lines

    def load_fpds_from_file(self, file_path):
        """
        Loads arbitrary set of ids, WITHOUT checking for deletes.
        :param file_path:
        :return:
        """
        with RetrieveFileFromUri(file_path).get_file_object() as file:
            for next_batch in self.next_file_batch_generator(file):
                id_list = [int(re.search(r"\d+", x).group()) for x in next_batch]
                logger.info(
                    "Loading next batch from provided file (size: {}, ids {}-{})...".format(
                        len(id_list), id_list[0], id_list[-1]
                    )
                )
                self.modified_award_ids.extend(load_ids(id_list))

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
            help="Script will load or reload all FPDS records in broker database, from all time. This does NOT clear the USASpending database first",
        )

    def handle(self, *args, **options):

        # Record script execution start time to update the FPDS last updated date in DB as appropriate
        update_time = datetime.now(timezone.utc)

        if options["reload_all"]:
            self.load_fpds_incrementally(None)

        elif options["date"]:
            self.load_fpds_incrementally(options["date"])

        elif options["ids"]:
            self.modified_award_ids.extend(load_ids(options["ids"]))

        elif options["file"]:
            self.load_fpds_from_file(options["file"])

        elif options["since_last_load"]:
            last_load = get_last_load_date("fpds")
            if not last_load:
                raise ValueError("No last load date for FPDS stored in the database")
            self.load_fpds_incrementally(last_load)

        if self.modified_award_ids:
            logger.info("cleaning orphaned metadata")
            destroy_orphans()
            logger.info(
                "updating award values ({} awards touched by transaction modifications)".format(
                    len(self.modified_award_ids)
                )
            )
            logger.info("{} awards touched by update_awards()".format(update_awards(tuple(self.modified_award_ids))))
            logger.info(
                "{} awards touched by update_contract_awards()".format(
                    update_contract_awards(tuple(self.modified_award_ids))
                )
            )
            update_c_to_d_linkages("contract")

        if failed_ids:
            logger.error(
                "The following detached_award_procurement_ids failed to load: {}".format(
                    ["{},".format(failure) for failure in failed_ids]
                )
            )
            SystemExit(1)

        if options["reload_all"] or options["since_last_load"]:
            # we wait until after the load finishes to update the load date because if this crashes we'll need to load again
            update_last_load_date("fpds", update_time)
