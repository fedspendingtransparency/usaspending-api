import logging

from datetime import datetime, timezone
from django.conf import settings
from django.core.management.base import BaseCommand
from time import perf_counter

from usaspending_api.broker.helpers.last_load_date import get_last_load_date
from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client
from usaspending_api.common.elasticsearch.elasticsearch_sql_helpers import ensure_view_exists
from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type
from usaspending_api.etl.elasticsearch_loader_helpers import (
    format_log,
    Controller,
    transform_award_data,
    transform_transaction_data,
    toggle_refresh_off,
)

logger = logging.getLogger("script")


class Command(BaseCommand):
    """Parallelized ETL script for indexing SQL data into Elasticsearch"""

    help = """Hopefully the code comments are helpful enough to figure this out...."""

    def add_arguments(self, parser):
        parser.add_argument(
            "--process-deletes",
            action="store_true",
            help="When this flag is set, the script will include the extra steps"
            "to calculate deleted records and remove from the target index",
        )
        parser.add_argument(
            "--index-name",
            type=str,
            help="Provide name for new index about to be created. Only used when --create-new-index is provided",
            metavar="",
        )
        parser.add_argument(
            "--create-new-index",
            action="store_true",
            help="It needs a new unique index name and set aliases used by API logic to the new index",
        )
        parser.add_argument(
            "--start-datetime",
            type=datetime_command_line_argument_type(naive=False),
            help="Processes transactions updated on or after the UTC date/time provided. yyyy-mm-dd hh:mm:ss is always "
            "a safe format. Wrap in quotes if date/time contains spaces.",
            metavar="",
        )
        parser.add_argument(
            "--skip-delete-index",
            action="store_true",
            help="When creating a new index, skip the steps that delete old indexes and swap aliases. "
            "Only applicable when --create-new-index is provided.",
        )
        parser.add_argument(
            "--load-type",
            type=str,
            required=True,
            help="Select which data the ETL will process.",
            choices=["transactions", "awards"],
        )
        parser.add_argument(
            "--processes",
            type=int,
            help="Number of parallel processes to operate. psycopg2 kicked the bucket with 100.",
            default=10,
            choices=range(1, 71),
            metavar="[1-70]",
        )
        parser.add_argument(
            "--partition-size",
            type=int,
            help="Set the target size of a single data partition. A partition "
            "might be slightly larger or slightly smaller depending on the "
            " distribution of the data to process",
            default=250000,
            metavar="(default: 250,000)",
        )

    def handle(self, *args, **options):
        elasticsearch_client = instantiate_elasticsearch_client()
        config = parse_cli_args(options, elasticsearch_client)

        start = perf_counter()
        logger.info(format_log(f"Starting script\n{'=' * 56}"))
        start_msg = "target index: {index_name} | Starting from: {starting_date}"
        logger.info(format_log(start_msg.format(**config)))

        ensure_view_exists(config["sql_view"])
        error_addition = ""
        loader = Controller(config)

        if config["is_incremental_load"]:
            toggle_refresh_off(elasticsearch_client, config["index_name"])  # Turned back on at end.

        try:
            loader.prepare_for_etl()
            loader.dispatch_tasks()
        except Exception as e:
            logger.error(f"{str(e)}")
            error_addition = "before encountering a problem during execution.... "
            raise SystemExit(1)
        else:
            loader.complete_process()
        finally:
            msg = f"Script duration was {perf_counter() - start:.2f}s {error_addition}|"
            headers = f"{'-' * (len(msg) - 2)} |"
            logger.info(format_log(headers))
            logger.info(format_log(msg))
            logger.info(format_log(headers))


def parse_cli_args(options: dict, es_client) -> dict:
    default_datetime = datetime.strptime(f"{settings.API_SEARCH_MIN_DATE}+0000", "%Y-%m-%d%z")
    passthrough_values = (
        "partition_size",
        "create_new_index",
        "index_name",
        "load_type",
        "process_deletes",
        "skip_counts",
        "skip_delete_index",
        "processes",
    )
    config = set_config(passthrough_values, options)

    if config["create_new_index"] and not config["index_name"]:
        raise SystemExit("Fatal error: '--create-new-index' requires '--index-name'.")
    elif config["create_new_index"]:
        config["index_name"] = config["index_name"].lower()
        config["starting_date"] = default_datetime
        check_new_index_name_is_ok(config["index_name"], config["required_index_name"])
    elif options["start_datetime"]:
        config["starting_date"] = options["start_datetime"]
    else:
        # Due to the queries used for fetching postgres data,
        #  `starting_date` needs to be present and a date before:
        #      - The earliest records in S3.
        #      - When all transaction records in the USAspending SQL database were updated.
        #   And keep it timezone-aware for S3
        config["starting_date"] = get_last_load_date(config["stored_date_key"], default=default_datetime)

    config["is_incremental_load"] = not bool(config["create_new_index"]) and (
        config["starting_date"] != default_datetime
    )

    if config["is_incremental_load"]:
        if config["index_name"]:
            logger.info(format_log(f"Ignoring provided index name, using alias '{config['write_alias']}' for safety"))
        config["index_name"] = config["write_alias"]
        if not es_client.cat.aliases(name=config["write_alias"]):
            logger.error(f"Write alias '{config['write_alias']}' is missing")
            raise SystemExit(1)
    else:
        if es_client.indices.exists(config["index_name"]):
            logger.error(f"Data load into existing index. Change index name or run an incremental load")
            raise SystemExit(1)

    if config["starting_date"] < default_datetime:
        logger.error(f"--start-datetime is too early. Set no earlier than {default_datetime}")
        raise SystemExit(1)

    return config


def set_config(passthrough_values: list, arg_parse_options: dict) -> dict:
    """Set values based on env vars and when the script started"""

    if arg_parse_options["load_type"] == "awards":
        config = {
            "base_table": "awards",
            "base_table_id": "id",
            "data_transform_func": transform_award_data,
            "data_type": "award",
            "max_query_size": settings.ES_AWARDS_MAX_RESULT_WINDOW,
            "primary_key": "award_id",
            "query_alias_prefix": settings.ES_AWARDS_QUERY_ALIAS_PREFIX,
            "required_index_name": settings.ES_AWARDS_NAME_SUFFIX,
            "sql_view": settings.ES_AWARDS_ETL_VIEW_NAME,
            "stored_date_key": "es_awards",
            "unique_key_field": "generated_unique_award_id",
            "write_alias": settings.ES_AWARDS_WRITE_ALIAS,
        }
    elif arg_parse_options["load_type"] == "transactions":
        config = {
            "base_table": "transaction_normalized",
            "base_table_id": "id",
            "data_transform_func": transform_transaction_data,
            "data_type": "transaction",
            "max_query_size": settings.ES_TRANSACTIONS_MAX_RESULT_WINDOW,
            "primary_key": "transaction_id",
            "query_alias_prefix": settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX,
            "required_index_name": settings.ES_TRANSACTIONS_NAME_SUFFIX,
            "sql_view": settings.ES_TRANSACTIONS_ETL_VIEW_NAME,
            "stored_date_key": "es_transactions",
            "unique_key_field": "generated_unique_transaction_id",
            "write_alias": settings.ES_TRANSACTIONS_WRITE_ALIAS,
        }
    else:
        raise RuntimeError(f"Configuration is not configured for --load-type={arg_parse_options['load_type']}")

    config.update({k: v for k, v in arg_parse_options.items() if k in passthrough_values})
    config.update(
        {
            "aws_region": settings.USASPENDING_AWS_REGION,
            "s3_bucket": settings.DELETED_TRANSACTION_JOURNAL_FILES,
            "processing_start_datetime": datetime.now(timezone.utc),
            "verbose": arg_parse_options["verbosity"] > 1,  # convert command's levels of verbosity to a bool
        }
    )

    return config


def check_new_index_name_is_ok(provided_name: str, suffix: str) -> None:
    if not provided_name.endswith(suffix):
        raise SystemExit(f"new index name doesn't end with the expected pattern: '{suffix}'")
