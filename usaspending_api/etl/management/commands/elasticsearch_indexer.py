import logging

from datetime import datetime, timezone
from django.conf import settings
from django.core.management.base import BaseCommand
from time import perf_counter

from usaspending_api.broker.helpers.last_load_date import get_last_load_date
from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client
from usaspending_api.common.elasticsearch.elasticsearch_sql_helpers import ensure_view_exists, drop_etl_view
from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type
from usaspending_api.etl.elasticsearch_loader_helpers import (
    Controller,
    execute_sql_statement,
    format_log,
    toggle_refresh_off,
    transform_award_data,
    transform_covid19_faba_data,
    transform_transaction_data,
)

logger = logging.getLogger("script")


class Command(BaseCommand):
    """Parallelized ETL script for indexing SQL data into Elasticsearch

    1. DB extraction should be very fast if the query is straightforward.
        We have seen 1-2 seconds or less per 10k rows on a db.r5.4xl instance with 10K IOPS
    2. Generally speaking, parallelization performance is largely based on number
        of vCPUs available to the pool of parallel processes. Ideally have 1 vCPU
        per process. This is mostly true for CPU-heavy tasks. If the ETL is primarily
        I/O then the number of processes per vCPU can be significantly increased.
    3. Elasticsearch indexing appears to become a bottleneck when the prior
        2 parts are taken care of. Further simplifying SQL, increasing the
        DB size, and increasing the worker node vCPUs/memory yielded the same
        overall runtime, due to ES indexing backpressure. Presumably adding more
        nodes, or increasing node size in the ES cluster may reduce this pressure,
        but not (yet) tested.
    """

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
            choices=["transaction", "award", "covid19-faba"],
        )
        parser.add_argument(
            "--processes",
            type=int,
            help="Number of parallel processes to operate. psycopg2 kicked the bucket with 100.",
            default=10,
            choices=range(1, 101),
            metavar="[1-100]",
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
        parser.add_argument(
            "--drop-db-view",
            action="store_true",
            help="After completing the ETL, drop the SQL view used for the data extraction",
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
            if config["drop_db_view"]:
                logger.info(format_log(f"Dropping SQL view '{config['sql_view']}'"))
                drop_etl_view(config["sql_view"], True)
        finally:
            msg = f"Script duration was {perf_counter() - start:.2f}s {error_addition}|"
            headers = f"{'-' * (len(msg) - 2)} |"
            logger.info(format_log(headers))
            logger.info(format_log(msg))
            logger.info(format_log(headers))


def parse_cli_args(options: dict, es_client) -> dict:
    passthrough_values = (
        "create_new_index",
        "drop_db_view",
        "index_name",
        "load_type",
        "partition_size",
        "process_deletes",
        "processes",
        "skip_counts",
        "skip_delete_index",
    )
    config = set_config(passthrough_values, options)

    if config["create_new_index"] and not config["index_name"]:
        raise SystemExit("Fatal error: '--create-new-index' requires '--index-name'.")
    elif config["create_new_index"]:
        config["index_name"] = config["index_name"].lower()
        config["starting_date"] = config["initial_datetime"]
        check_new_index_name_is_ok(config["index_name"], config["required_index_name"])
    elif options["start_datetime"]:
        config["starting_date"] = options["start_datetime"]
    else:
        # Due to the queries used for fetching postgres data,
        #  `starting_date` needs to be present and a date before:
        #      - The earliest records in S3.
        #      - When all transaction records in the USAspending SQL database were updated.
        #   And keep it timezone-aware for S3
        config["starting_date"] = get_last_load_date(config["stored_date_key"], default=config["initial_datetime"])

    config["is_incremental_load"] = not bool(config["create_new_index"]) and (
        config["starting_date"] != config["initial_datetime"]
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

    if config["starting_date"] < config["initial_datetime"]:
        logger.error(f"--start-datetime is too early. Set no earlier than {config['initial_datetime']}")
        raise SystemExit(1)

    return config


def set_config(passthrough_values: list, arg_parse_options: dict) -> dict:
    """Set values based on env vars and when the script started"""
    default_datetime = datetime.strptime(f"{settings.API_SEARCH_MIN_DATE}+0000", "%Y-%m-%d%z")
    if arg_parse_options["load_type"] == "award":
        config = {
            "base_table": "awards",
            "base_table_id": "id",
            "create_award_type_aliases": True,
            "data_transform_func": transform_award_data,
            "data_type": "award",
            "execute_sql_func": execute_sql_statement,
            "extra_null_partition": False,
            "field_for_es_id": "award_id",
            "initial_datetime": default_datetime,
            "max_query_size": settings.ES_AWARDS_MAX_RESULT_WINDOW,
            "optional_predicate": """WHERE "update_date" >= '{starting_date}'""",
            "primary_key": "award_id",
            "query_alias_prefix": settings.ES_AWARDS_QUERY_ALIAS_PREFIX,
            "required_index_name": settings.ES_AWARDS_NAME_SUFFIX,
            "sql_view": settings.ES_AWARDS_ETL_VIEW_NAME,
            "stored_date_key": "es_awards",
            "unique_key_field": "generated_unique_award_id",
            "write_alias": settings.ES_AWARDS_WRITE_ALIAS,
        }
    elif arg_parse_options["load_type"] == "transaction":
        config = {
            "base_table": "transaction_normalized",
            "base_table_id": "id",
            "create_award_type_aliases": True,
            "data_transform_func": transform_transaction_data,
            "data_type": "transaction",
            "execute_sql_func": execute_sql_statement,
            "extra_null_partition": False,
            "field_for_es_id": "transaction_id",
            "initial_datetime": default_datetime,
            "max_query_size": settings.ES_TRANSACTIONS_MAX_RESULT_WINDOW,
            "optional_predicate": """WHERE "etl_update_date" >= '{starting_date}'""",
            "primary_key": "transaction_id",
            "query_alias_prefix": settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX,
            "required_index_name": settings.ES_TRANSACTIONS_NAME_SUFFIX,
            "sql_view": settings.ES_TRANSACTIONS_ETL_VIEW_NAME,
            "stored_date_key": "es_transactions",
            "unique_key_field": "generated_unique_transaction_id",
            "write_alias": settings.ES_TRANSACTIONS_WRITE_ALIAS,
        }
    elif arg_parse_options["load_type"] == "covid19-faba":
        config = {
            "base_table": "financial_accounts_by_awards",
            "base_table_id": "financial_accounts_by_awards_id",
            "create_award_type_aliases": False,
            "data_transform_func": transform_covid19_faba_data,
            "data_type": "covid19-faba",
            "execute_sql_func": execute_sql_statement,
            "extra_null_partition": True,
            "field_for_es_id": "financial_account_distinct_award_key",
            "initial_datetime": datetime.strptime(f"2020-04-01+0000", "%Y-%m-%d%z"),
            "max_query_size": settings.ES_COVID19_FABA_MAX_RESULT_WINDOW,
            "optional_predicate": "",
            "primary_key": "award_id",
            "query_alias_prefix": settings.ES_COVID19_FABA_QUERY_ALIAS_PREFIX,
            "required_index_name": settings.ES_COVID19_FABA_NAME_SUFFIX,
            "sql_view": settings.ES_COVID19_FABA_ETL_VIEW_NAME,
            "stored_date_key": ...,
            "unique_key_field": "distinct_award_key",
            "write_alias": settings.ES_COVID19_FABA_WRITE_ALIAS,
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
