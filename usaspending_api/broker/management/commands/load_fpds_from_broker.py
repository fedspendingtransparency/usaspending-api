from django.core.management.base import BaseCommand
import logging
import re
import psycopg2
from datetime import datetime, timezone

from usaspending_api.data_load.fpds_loader import run_fpds_load, destroy_orphans, load_chunk
from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri
from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type
from usaspending_api.common.helpers.sql_helpers import get_broker_dsn_string
from usaspending_api.common.helpers.etl_helpers import update_c_to_d_linkages
from usaspending_api.etl.award_helpers import update_awards, update_contract_awards
from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date

logger = logging.getLogger("console")

BROKER_CONNECTION_STRING = get_broker_dsn_string()

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
            db_query = ALL_FPDS_QUERY.format("*")

        if date:
            db_cursor.execute(db_query + " WHERE updated_at >= %s;", [date])
        else:
            db_cursor.execute(db_query)
        return db_cursor

    def load_fpds_from_date(self, date):
        if date is None:
            logger.info("fetching all fpds transactions...")
        else:
            logger.info("fetching fpds transactions since {}...".format(str(date)))
        with psycopg2.connect(dsn=BROKER_CONNECTION_STRING) as connection:
            total_records = self.get_cursor_for_date_query(connection, date, True).fetchall()[0][0]
            records_processed = 0
            logger.info("{} total records".format(total_records))
            cursor = self.get_cursor_for_date_query(connection, date)
            while True:
                id_list = cursor.fetchmany(CHUNK_SIZE)
                if len(id_list) == 0:
                    break
                logger.info("Loading batch from date query (size: {})...".format(len(id_list)))
                self.modified_award_ids.extend(load_chunk(id_list))
                records_processed = records_processed + len(id_list)
                logger.info("{} out of {} processed".format(records_processed, total_records))

    @staticmethod
    def next_file_batch_generator(file):
        while True:
            lines = file.readlines(CHUNK_SIZE)
            lines = [line.decode("utf-8") for line in lines]
            if len(lines) == 0:
                break
            yield lines

    def load_fpds_from_file(self, file_path):
        with RetrieveFileFromUri(file_path).get_file_object() as file:
            for next_batch in self.next_file_batch_generator(file):
                id_list = [int(re.search(r"\d+", x).group()) for x in next_batch]
                logger.info(
                    "Loading next batch from provided file (size: {}, ids {}-{})...".format(
                        len(id_list), id_list[0], id_list[-1]
                    )
                )
                self.modified_award_ids.extend(run_fpds_load(id_list))

    def add_arguments(self, parser):
        mutually_exclusive_group = parser.add_mutually_exclusive_group(required=True)

        mutually_exclusive_group.add_argument(
            "--ids",
            nargs="+",
            type=int,
            help="(OPTIONAL) detached_award_procurement_ids of FPDS transactions to load/reload from Broker",
        )
        mutually_exclusive_group.add_argument(
            "--date",
            dest="date",
            type=datetime_command_line_argument_type(naive=True),  # Broker date/times are naive.
            help="Load or Reload all FPDS records from the provided date to the current time. YYYY-MM-DD format",
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
            help="A file containing only transaction IDs (detached_award_procurement_id) "
            "to reload, one ID per line. Nonexistent IDs will be ignored.",
        )
        mutually_exclusive_group.add_argument(
            "--reload-all",
            action="store_true",
            help="Script will load or reload all FPDS records in broker database, from all time",
        )

    def handle(self, *args, **options):

        # loads can take a while, so we record last updated date from the start of all transactions
        last_update_time = datetime.now(timezone.utc)

        if options["reload_all"]:
            self.load_fpds_from_date(None)

        elif options["date"]:
            self.load_fpds_from_date(options["date"])

        elif options["ids"]:
            self.modified_award_ids.extend(run_fpds_load(options["ids"]))

        elif options["file"]:
            self.load_fpds_from_file(options["file"])

        elif options["since_last_load"]:
            self.load_fpds_from_date(get_last_load_date("fpds"))

        if options["reload_all"] or options["since_last_load"]:
            # we wait until after the load finishes to update the load date because if this crashes we'll need to load again
            update_last_load_date("fpds", last_update_time)

        if self.modified_award_ids:
            logger.info("cleaning orphaned rows")
            destroy_orphans()
            logger.info("updating award values ({} awards modified)".format(len(self.modified_award_ids)))
            update_awards(tuple(self.modified_award_ids))
            update_contract_awards(tuple(self.modified_award_ids))
            update_c_to_d_linkages("contract")
