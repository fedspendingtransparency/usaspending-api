import logging

from datetime import datetime, timezone
from django.core.management.base import BaseCommand
from django.db import connection

from usaspending_api.broker import lookups
from usaspending_api.broker.helpers.delete_fabs_transactions import delete_fabs_transactions
from usaspending_api.broker.helpers.last_load_date import update_last_load_date
from usaspending_api.broker.helpers.upsert_fabs_transactions import upsert_fabs_transactions
from usaspending_api.broker.models import ExternalDataLoadDate
from usaspending_api.common.helpers.date_helper import cast_datetime_to_naive, datetime_command_line_argument_type
from usaspending_api.common.helpers.timing_helpers import timer
from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri
from usaspending_api.transactions.transaction_delete_journal_helpers import retrieve_deleted_fabs_transactions

logger = logging.getLogger("script")


SUBMISSION_LOOKBACK_MINUTES = 15


def get_last_load_date():
    """
    Wraps the get_load_load_date helper which is responsible for grabbing the
    last load date from the database.

    Without getting into too much detail, SUBMISSION_LOOKBACK_MINUTES is used
    to counter a very rare race condition where database commits are saved ever
    so slightly out of order with the updated_at timestamp.  It will be
    subtracted from the last_run_date to ensure submissions with similar
    updated_at times do not fall through the cracks.  An unfortunate side
    effect is that some submissions may be processed more than once which
    SHOULDN'T cause any problems but will add to the run time.  To minimize
    this, keep the value as small as possible while still preventing skips.  To
    be clear, the original fabs loader did this as well, just in a way that did
    not always prevent skips.
    """
    from usaspending_api.broker.helpers.last_load_date import get_last_load_date

    last_load_date = get_last_load_date("fabs", SUBMISSION_LOOKBACK_MINUTES)
    if last_load_date is None:
        external_data_type_id = lookups.EXTERNAL_DATA_TYPE_DICT["fabs"]
        raise RuntimeError(
            "Unable to find last_load_date in table {} for external_data_type_id={}. "
            "If this is expected and the goal is to reload all submissions, supply the "
            "--reload-all switch on the command line.".format(
                ExternalDataLoadDate.objects.model._meta.db_table, external_data_type_id
            )
        )
    return last_load_date


def _get_ids(sql, afa_ids, start_datetime, end_datetime):
    params = []
    if afa_ids:
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


def get_fabs_transaction_ids(afa_ids, start_datetime, end_datetime):
    sql = """
        select  published_award_financial_assistance_id
        from    source_assistance_transaction
        where   is_active is true
    """
    ids = _get_ids(sql, afa_ids, start_datetime, end_datetime)
    logger.info("Number of records to insert/update: {:,}".format(len(ids)))
    return ids


def read_afa_ids_from_file(afa_id_file_path):
    with RetrieveFileFromUri(afa_id_file_path).get_file_object() as f:
        return tuple(l.decode("utf-8").rstrip() for l in f if l)


class Command(BaseCommand):
    help = (
        "Update FABS data in USAspending from source data. Command line parameters "
        "are ANDed together so, for example, providing a transaction id and a "
        "submission id that do not overlap will result no new FABs records."
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
            "--afa-id-file",
            metavar="FILEPATH",
            type=str,
            help="A file containing only broker transaction IDs (afa_generated_unique) "
            "to reload, one ID per line. Nonexistent IDs will be ignored.",
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
            afa_ids = None
            start_datetime = None
            end_datetime = None
        else:
            afa_ids = read_afa_ids_from_file(options["afa_id_file"]) if options["afa_id_file"] else None
            start_datetime = options["start_datetime"]
            end_datetime = options["end_datetime"]

        # If no other processing options were provided than this is an incremental load.
        is_incremental_load = not any((reload_all, afa_ids, start_datetime, end_datetime))

        if is_incremental_load:
            start_datetime = get_last_load_date()
            logger.info("Processing data for FABS starting from %s" % start_datetime)

        with timer("obtaining delete records", logger.info):
            delete_records = retrieve_deleted_fabs_transactions(start_datetime, end_datetime)
            ids_to_delete = [item for sublist in delete_records.values() for item in sublist if item]
        logger.info(f"{len(ids_to_delete):,} delete ids found in total")

        with timer("retrieving/diff-ing FABS Data", logger.info):
            ids_to_upsert = get_fabs_transaction_ids(afa_ids, start_datetime, end_datetime)

        update_award_ids = delete_fabs_transactions(ids_to_delete)
        upsert_fabs_transactions(ids_to_upsert, update_award_ids)

        if is_incremental_load:
            update_last_load_date("fabs", processing_start_datetime)

        logger.info("FABS UPDATE FINISHED!")
