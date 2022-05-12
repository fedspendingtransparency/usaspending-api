import logging

from datetime import datetime, timedelta, timezone
from django.core.management.base import BaseCommand
from django.db import connection
from django.db.models import Max

from usaspending_api.awards.models import TransactionFABS
from usaspending_api.broker import lookups
from usaspending_api.broker.helpers.delete_fabs_transactions import (
    delete_fabs_transactions,
    get_delete_pks_for_afa_keys,
)
from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date
from usaspending_api.broker.helpers.upsert_fabs_transactions import upsert_fabs_transactions
from usaspending_api.broker.models import ExternalDataLoadDate
from usaspending_api.common.helpers.date_helper import cast_datetime_to_naive, datetime_command_line_argument_type
from usaspending_api.common.helpers.timing_helpers import timer
from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri
from usaspending_api.transactions.transaction_delete_journal_helpers import retrieve_deleted_fabs_transactions


logger = logging.getLogger("script")


LAST_LOAD_LOOKBACK_MINUTES = 15
UPDATED_AT_MODIFIER_MS = 1


def get_incremental_load_start_datetime():
    """
    This function is designed to help prevent two issues we've discovered with the FABS nightly
    pipeline:

     #1 LAST_LOAD_LOOKBACK_MINUTES are subtracted from last load datetime to counter a very rare
        race condition where database commits are saved ever so slightly out of order when compared
        to the last updated timestamp due to commit duration which can cause transactions to be
        skipped.  The timestamp is set to when the commit begins, so if the commit starts before
        the load but doesn't finish until after the load, the transactions saved as part of that
        commit will never get picked up.  This has happened in the wild at least once and is not
        all that hard to imagine if you consider that large submissions can take many minutes to
        commit.  We do not currently have a good way to test this, but I've seen extremely large
        transactions take an hour to commit.  I do not believe submissions currently get this large
        but it's something to keep in mind.

     #2 We use the minimum of the last load date or the max transaction_fabs updated_at date
        to prevent FABS transactions submitted between when the source records are copied from
        Broker and when FABS transactions are processed from being skipped.

    An unfortunate side effect of the look back is that some submissions may be processed more than
    once.  This SHOULDN'T cause any problems since the FABS loader is designed to be able to reload
    transactions, but it could add to the run time.  To minimize reprocessing, keep the
    LAST_LOAD_LOOKBACK_MINUTES value as small as possible while still preventing skips.  To be
    clear, the original fabs loader did this as well, just in a way that did not always prevent
    skips (by always running since midnight - which had its own issues).
    """
    last_load_date = get_last_load_date("fabs", LAST_LOAD_LOOKBACK_MINUTES)
    if last_load_date is None:
        raise RuntimeError(
            f"Unable to find last_load_date in table {ExternalDataLoadDate.objects.model._meta.db_table} "
            f"for external_data_type_id={lookups.EXTERNAL_DATA_TYPE_DICT['fabs']}.  If this is expected and "
            f"the goal is to reload all submissions, supply the --reload-all switch on the command line."
        )
    max_updated_at = TransactionFABS.objects.aggregate(Max("updated_at"))["updated_at__max"]
    if max_updated_at is None:
        return last_load_date
    else:
        logger.info(f"Most recent update_date in `transaction_fabs` {max_updated_at}")

    # We add a little tiny bit of time to the max_updated_at to prevent us from always reprocessing
    # records since the SQL that grabs new records is using updated_at >=.  I realize this is a hack
    # but the pipeline is already running for too long so anything we can do to prevent elongating
    # it should be welcome.
    max_updated_at += timedelta(milliseconds=UPDATED_AT_MODIFIER_MS)

    return min((last_load_date, max_updated_at))


def _get_ids(sql, ids, afa_ids, start_datetime, end_datetime):
    params = []
    if ids and afa_ids:
        sql += " and (published_fabs_id in %s or afa_generated_unique in %s)"
        params.append(tuple(ids))
        params.append(tuple(afa_ids))
    elif ids:
        sql += " and published_fabs_id in %s"
        params.append(tuple(ids))
    elif afa_ids:
        sql += " and afa_generated_unique in %s"
        params.append(tuple(afa_ids))
    if start_datetime:
        sql += " and updated_at >= %s"
        params.append(cast_datetime_to_naive(start_datetime))
    if end_datetime:
        sql += " and updated_at < %s"
        params.append(cast_datetime_to_naive(end_datetime))
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return tuple(row[0] for row in cursor.fetchall())


def get_fabs_transaction_ids(ids, afa_ids, start_datetime, end_datetime):
    sql = """
        select  published_fabs_id
        from    source_assistance_transaction
        where   is_active is true
    """
    ids = _get_ids(sql, ids, afa_ids, start_datetime, end_datetime)
    logger.info("Number of records to insert/update: {:,}".format(len(ids)))
    return ids


def read_afa_ids_from_file(afa_id_file_path):
    with RetrieveFileFromUri(afa_id_file_path).get_file_object() as f:
        return {line.decode("utf-8").rstrip() for line in f if line}


class Command(BaseCommand):
    help = (
        "Update FABS data in USAspending from source data. Providing no options "
        "performs an incremental (from last run) load. Deletes are only executed "
        "with incremental loads."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--reload-all",
            action="store_true",
            help="Reload all FABS transactions. Does not clear out USAspending "
            "FABS transactions beforehand. If omitted, all submissions from "
            "the last successful run will be loaded. THIS SETTING SUPERSEDES "
            "ALL OTHER PROCESSING OPTIONS.",
        )

        parser.add_argument(
            "--ids",
            default=[],
            metavar="ID",
            nargs="+",
            type=int,
            help="A list of Broker transaction IDs (published_fabs_id) to reload. IDs "
            "provided here will be combined with any --afa-id-file and --afa-ids "
            "IDs provided. Nonexistent IDs will be ignored.",
        )

        parser.add_argument(
            "--afa-ids",
            default=[],
            metavar="AFA_ID",
            nargs="+",
            help="A list of Broker transaction IDs (afa_generated_unique) to "
            "reload. IDs provided here will be combined with any --afa-id-file and --ids "
            "IDs provided. Nonexistent IDs will be ignored. If any AFA IDs start "
            "with a dash or other special shell character, use the --afa-id-file "
            "option.",
        )

        parser.add_argument(
            "--afa-id-file",
            metavar="FILEPATH",
            help="A file containing only Broker transaction IDs (afa_generated_unique) "
            "to reload, one ID per line. IDs provided here will be combined with any "
            "--afa-ids and --ids IDs provided. Nonexistent IDs will be ignored.",
        )

        parser.add_argument(
            "--start-datetime",
            type=datetime_command_line_argument_type(naive=True),  # Broker date/times are naive.
            help="Processes transactions updated on or after the UTC date/time "
            "provided. yyyy-mm-dd hh:mm:ss is always a safe format. Wrap in "
            "quotes if date/time contains spaces.",
        )

        parser.add_argument(
            "--end-datetime",
            type=datetime_command_line_argument_type(naive=True),  # Broker date/times are naive.
            help="Processes transactions updated prior to the UTC date/time "
            "provided. yyyy-mm-dd hh:mm:ss is always a safe format. Wrap in "
            "quotes if date/time contains spaces.",
        )

    def handle(self, *args, **options):
        processing_start_datetime = datetime.now(timezone.utc)

        logger.info("Starting FABS data load script...")

        # "Reload all" supersedes all other processing options.
        reload_all = options["reload_all"]
        if reload_all:
            ids = None
            afa_ids = None
            start_datetime = None
            end_datetime = None
        else:
            ids = options["ids"]
            afa_ids = set(options["afa_ids"])
            if options["afa_id_file"]:
                afa_ids = tuple(afa_ids | read_afa_ids_from_file(options["afa_id_file"]))
            start_datetime = options["start_datetime"]
            end_datetime = options["end_datetime"]

        # If no other processing options were provided than this is an incremental load.
        is_incremental_load = not any((reload_all, ids, afa_ids, start_datetime, end_datetime))

        if is_incremental_load:
            start_datetime = get_incremental_load_start_datetime()
            logger.info(f"Processing data for FABS starting from {start_datetime} (includes offset)")

            # We only perform deletes with incremental loads.
            with timer("obtaining delete records", logger.info):
                delete_records = retrieve_deleted_fabs_transactions(start_datetime, end_datetime)
                ids_to_delete = [item for sublist in delete_records.values() for item in sublist if item]
                ids_to_delete = get_delete_pks_for_afa_keys(ids_to_delete)
            logger.info(f"{len(ids_to_delete):,} delete ids found in total")

        with timer("retrieving IDs of FABS to process", logger.info):
            ids_to_upsert = get_fabs_transaction_ids(ids, afa_ids, start_datetime, end_datetime)

        update_and_delete_award_ids = delete_fabs_transactions(ids_to_delete) if is_incremental_load else []
        upsert_fabs_transactions(ids_to_upsert, update_and_delete_award_ids)

        if is_incremental_load:
            logger.info(f"Storing {processing_start_datetime} for the next incremental run")
            update_last_load_date("fabs", processing_start_datetime)

        logger.info("FABS UPDATE FINISHED!")
