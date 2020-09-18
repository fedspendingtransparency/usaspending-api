import logging

from datetime import datetime, timezone
from django.conf import settings
from django.core.management.base import BaseCommand
from pathlib import Path
from time import perf_counter

from usaspending_api.broker.helpers.last_load_date import get_last_load_date
from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client
from usaspending_api.common.elasticsearch.elasticsearch_sql_helpers import ensure_view_exists
from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type, fy as parse_fiscal_year
from usaspending_api.common.helpers.fiscal_year_helpers import create_fiscal_year_list
from usaspending_api.etl.elasticsearch_loader_helpers.es_etl_helpers import format_log
from usaspending_api.etl.elasticsearch_loader_helpers.rapidloader import Rapidloader

logger = logging.getLogger("script")


class Command(BaseCommand):
    """ETL script for indexing transaction data into Elasticsearch

    HIGHLEVEL PROCESS OVERVIEW
         1. Generate the full list of fiscal years to process as jobs
         2. Iterate by job
           a. Download a CSV file by year (one at a time)
               i. Continue to download a CSV file until all years are downloaded
           b. Upload a CSV to Elasticsearch
               i. Continue to upload a CSV file until all years are uploaded to ES
           c. Delete CSV file
    TO RELOAD ALL data:
        python3 manage.py es_rapidloader --index-name <NEW-INDEX-NAME> --create-new-index all

        Running with --new-index will trigger several actions:
        0. A view will be created in the source database for the ETL queries
        1. A new index will be created from the value provided by --index-name (obviously)
        2. A new index template will be loaded into the cluster to set mapping and index metadata
        3. All aliases used by the API queries will be re-assigned to the new index
        4. An alias for incremental indexes will be applied to the new index
        5. If any previous indexes existed with the API aliases, they will be deleted.
    """

    help = """Hopefully the code comments are helpful enough to figure this out...."""

    def add_arguments(self, parser):
        parser.add_argument(
            "fiscal_years",
            nargs="+",
            type=str,
            metavar="fiscal-years",
            help="Provide a list of fiscal years to process. For convenience, provide 'all' for FY2008 to current FY",
        )
        parser.add_argument(
            "--process-deletes",
            action="store_true",
            help="When this flag is set, the script will include the process to "
            "obtain records of deleted transactions from S3 and remove from the index",
        )
        parser.add_argument(
            "--dir",
            default=str(Path(__file__).resolve().parent),
            type=str,
            help="Set for a custom location of output files",
            dest="directory",
        )
        parser.add_argument(
            "--skip-counts",
            action="store_true",
            help="When this flag is set, the ETL process will skip the record counts to reduce operation time",
        )
        parser.add_argument(
            "--index-name",
            type=str,
            help="Provide name for new index about to be created. Only used when --create-new-index is provided",
        )
        parser.add_argument(
            "--create-new-index",
            action="store_true",
            help="It needs a new unique index name and set aliases used by API logic to the new index",
        )
        parser.add_argument(
            "--snapshot",
            action="store_true",
            help="Create a new Elasticsearch snapshot of the current index state which is stored in S3",
        )
        parser.add_argument(
            "--start-datetime",
            type=datetime_command_line_argument_type(naive=False),
            help="Processes transactions updated on or after the UTC date/time provided. yyyy-mm-dd hh:mm:ss is always "
            "a safe format. Wrap in quotes if date/time contains spaces.",
        )
        parser.add_argument(
            "--skip-delete-index",
            action="store_true",
            help="When creating a new index skip the step that deletes the old indexes and swaps the aliases. "
            "Only used when --create-new-index is provided.",
        )
        parser.add_argument(
            "--load-type",
            type=str,
            help="Select which type of load to perform, current options are transactions or awards.",
            choices=["transactions", "awards"],
            default="transactions",
        )
        parser.add_argument(
            "--idle-wait-time",
            type=int,
            help="Time in seconds the ES index process should wait before looking for a new CSV data file.",
            default=60,
        )

    def handle(self, *args, **options):
        elasticsearch_client = instantiate_elasticsearch_client()
        config = process_cli_parameters(options, elasticsearch_client)

        start = perf_counter()
        logger.info(format_log(f"Starting script\n{'=' * 56}"))
        start_msg = "target index: {index_name} | FY(s): {fiscal_years} | Starting from: {starting_date}"
        logger.info(format_log(start_msg.format(**config)))

        if config["load_type"] == "transactions":
            ensure_view_exists(settings.ES_TRANSACTIONS_ETL_VIEW_NAME)
        elif config["load_type"] == "awards":
            ensure_view_exists(settings.ES_AWARDS_ETL_VIEW_NAME)

        loader = Rapidloader(config, elasticsearch_client)
        loader.run_load_steps()
        loader.complete_process()

        logger.info(format_log("---------------------------------------------------------------"))
        logger.info(format_log(f"Script completed in {perf_counter() - start:.2f}s"))
        logger.info(format_log("---------------------------------------------------------------"))


def process_cli_parameters(options: dict, es_client) -> dict:
    default_datetime = datetime.strptime(f"{settings.API_SEARCH_MIN_DATE}+0000", "%Y-%m-%d%z")
    simple_args = (
        "skip_delete_index",
        "process_deletes",
        "create_new_index",
        "snapshot",
        "index_name",
        "directory",
        "skip_counts",
        "load_type",
    )
    config = set_config(simple_args, options)

    config["fiscal_years"] = fiscal_years_for_processing(options)
    config["directory"] = Path(config["directory"]).resolve()

    if config["create_new_index"] and not config["index_name"]:
        raise SystemExit("Fatal error: --create-new-index requires --index-name.")
    elif config["create_new_index"]:
        config["index_name"] = config["index_name"].lower()
        config["starting_date"] = default_datetime
        check_new_index_name_is_ok(
            config["index_name"],
            settings.ES_AWARDS_NAME_SUFFIX if config["load_type"] == "awards" else settings.ES_TRANSACTIONS_NAME_SUFFIX,
        )
    elif options["start_datetime"]:
        config["starting_date"] = options["start_datetime"]
    else:
        # Due to the queries used for fetching postgres data,
        #  `starting_date` needs to be present and a date before:
        #      - The earliest records in S3.
        #      - When all transaction records in the USAspending SQL database were updated.
        #   And keep it timezone-award for S3
        config["starting_date"] = get_last_load_date(f"es_{options['load_type']}", default=default_datetime)

    config["max_query_size"] = settings.ES_TRANSACTIONS_MAX_RESULT_WINDOW
    if options["load_type"] == "awards":
        config["max_query_size"] = settings.ES_AWARDS_MAX_RESULT_WINDOW

    config["is_incremental_load"] = not bool(config["create_new_index"]) and (
        config["starting_date"] != default_datetime
    )

    if config["is_incremental_load"]:
        write_alias = settings.ES_TRANSACTIONS_WRITE_ALIAS
        if config["load_type"] == "awards":
            write_alias = settings.ES_AWARDS_WRITE_ALIAS
        if config["index_name"]:
            logger.info(format_log(f"Ignoring provided index name, using alias '{write_alias}' for incremental load"))
        config["index_name"] = write_alias
        if not es_client.cat.aliases(name=write_alias):
            logger.error(format_log(f"Write alias '{write_alias}' is missing"))
            raise SystemExit(1)
    else:
        if es_client.indices.exists(config["index_name"]):
            logger.error(format_log(f"Data load into existing index. Change index name or run an incremental load"))
            raise SystemExit(1)

    if not config["directory"].is_dir():
        logger.error(format_log(f"Provided directory does not exist"))
        raise SystemExit(1)
    elif config["starting_date"] < default_datetime:
        logger.error(format_log(f"--start-datetime is too early. Set no earlier than {default_datetime}"))
        raise SystemExit(1)
    elif not config["is_incremental_load"] and config["process_deletes"]:
        logger.error(format_log("Skipping deletions for ths load, --deleted overwritten to False"))
        config["process_deletes"] = False

    config["ingest_wait"] = options["idle_wait_time"]

    return config


def set_config(copy_args: list, arg_parse_options: dict) -> dict:
    """Set values based on env vars and when the script started"""
    root_index = settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX
    if arg_parse_options["load_type"] == "awards":
        root_index = settings.ES_AWARDS_QUERY_ALIAS_PREFIX
    config = {
        "aws_region": settings.USASPENDING_AWS_REGION,
        "s3_bucket": settings.DELETED_TRANSACTION_JOURNAL_FILES,
        "root_index": root_index,
        "processing_start_datetime": datetime.now(timezone.utc),
        "verbose": arg_parse_options["verbosity"] > 1,  # convert the management command's levels of verbosity to a bool
    }

    config.update({k: v for k, v in arg_parse_options.items() if k in copy_args})
    return config


def fiscal_years_for_processing(options: list) -> list:
    if "all" in options["fiscal_years"]:
        return create_fiscal_year_list(start_year=parse_fiscal_year(settings.API_SEARCH_MIN_DATE))
    return [int(x) for x in options["fiscal_years"]]


def check_new_index_name_is_ok(provided_name: str, suffix: str) -> None:
    if not provided_name.endswith(suffix):
        raise SystemExit(f"new index name doesn't end with the expected pattern: '{suffix}'")
