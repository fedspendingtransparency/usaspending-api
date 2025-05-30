import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from time import perf_counter

from django.conf import settings
from django.core.management.base import BaseCommand

from usaspending_api.broker.helpers.last_load_date import get_last_load_date
from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client
from usaspending_api.common.elasticsearch.elasticsearch_sql_helpers import drop_etl_view
from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type
from usaspending_api.etl.elasticsearch_loader_helpers import (
    check_new_index_name_is_ok,
    check_pipeline_dates,
    execute_sql_statement,
    format_log,
    toggle_refresh_off,
    transform_award_data,
    transform_location_data,
    transform_subaward_data,
    transform_transaction_data,
)
from usaspending_api.etl.elasticsearch_loader_helpers.controller import (
    AbstractElasticsearchIndexerController,
    PostgresElasticsearchIndexerController,
)
from usaspending_api.etl.elasticsearch_loader_helpers.index_config import (
    ES_AWARDS_UNIQUE_KEY_FIELD,
    ES_LOCATION_UNIQUE_KEY_FIELD,
    ES_RECIPIENT_UNIQUE_KEY_FIELD,
    ES_SUBAWARD_UNIQUE_KEY_FIELD,
    ES_TRANSACTIONS_UNIQUE_KEY_FIELD,
)

logger = logging.getLogger("script")


class AbstractElasticsearchIndexer(ABC, BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--process-deletes",
            action="store_true",
            help="When this flag is set, the script will include the extra steps"
            "to calculate deleted records and remove from the target index",
        )
        parser.add_argument(
            "--deletes-only",
            action="store_true",
            help="When this flag is set, the script will skip any steps not related"
            "to deleting records from target index",
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
            help="Processes transactions updated on or after the UTC date/time provided. yyyy-mm-dd hh:mm:ss "
            "is always a safe format. Wrap in quotes if date/time contains spaces. Does not apply to deletes",
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
            choices=["transaction", "award", "recipient", "location", "subaward"],
        )
        parser.add_argument(
            "--processes",
            type=int,
            help="Number of parallel processes to operate. psycopg2 kicked the bucket with 100. If running on Spark, "
            "this will be replaced by the number of executors on the configured Spark cluster",
            default=10,
            choices=range(1, 101),
            metavar="[1-100]",
        )
        parser.add_argument(
            "--partition-size",
            type=int,
            help="Set the batch-size of a single partition of data to process.",
            default=10000,
            metavar="(default: 10,000)",
        )
        parser.add_argument(
            "--drop-db-view",
            action="store_true",
            help="After completing the ETL, drop the SQL view used for the data extraction",
        )
        parser.add_argument(
            "--skip-date-check",
            action="store_true",
            help=(
                "When creating a new index it is verified that the es_deletes timestamp occurs after the earliest"
                " timestamp associated with the Transaction loader. If that check fails it is recommended that the"
                " es_deletes and other Elasticsearch timestamps are updated manually prior to proceeding."
            ),
        )

    def handle(self, *args, **options):
        elasticsearch_client = instantiate_elasticsearch_client()
        config = parse_cli_args(options, elasticsearch_client)

        start = perf_counter()
        logger.info(format_log(f"Starting script\n{'=' * 56}"))
        start_msg = "target index: {index_name} | Starting from: {starting_date}"
        logger.info(format_log(start_msg.format(**config)))

        error_addition = ""
        controller = self.create_controller(config)
        controller.ensure_view_exists(config["sql_view"])

        controller.set_slice_count()

        if config["is_incremental_load"]:
            toggle_refresh_off(elasticsearch_client, config["index_name"])  # Turned back on at end.

        try:
            if config["process_deletes"]:
                controller.run_deletes()

            if not config["deletes_only"]:
                controller.prepare_for_etl()
                controller.dispatch_tasks()
        except Exception as e:
            logger.error(f"{str(e)}")
            error_addition = "before encountering a problem during execution.... "
            raise SystemExit(1)
        else:
            controller.complete_process()
            if config["drop_db_view"]:
                logger.info(format_log(f"Dropping SQL view '{config['sql_view']}'"))
                drop_etl_view(config["sql_view"], True)
        finally:
            msg = f"Script duration was {perf_counter() - start:.2f}s {error_addition}|"
            headers = f"{'-' * (len(msg) - 2)} |"
            logger.info(format_log(headers))
            logger.info(format_log(msg))
            logger.info(format_log(headers))
            controller.cleanup()

        # Used to help pipeline determine when job passed but needs attention
        if config["raise_status_code_3"]:
            raise SystemExit(3)

    @abstractmethod
    def create_controller(self, config: dict) -> AbstractElasticsearchIndexerController:
        pass


class Command(AbstractElasticsearchIndexer):
    """Parallelized ETL script for indexing PostgreSQL data into Elasticsearch

    1. DB extraction should be very fast if the query is straightforward.
        We have seen 1-2 seconds or less per 10k rows on a db.r5.4xl instance with 10K IOPS
    2. Generally speaking, parallelization performance is largely based on number of vCPUs available to the
        pool of parallel processes. Ideally have 1 vCPU per process. This is mostly true for CPU-heavy tasks.
        If the ETL is primarily I/O then the number of processes per vCPU can be significantly increased.
    3. Elasticsearch indexing appears to become a bottleneck when the prior 2 parts are taken care of.
        Further simplifying SQL, increasing the DB size, and increasing the worker node vCPUs/memory
        yielded the same overall runtime, due to ES indexing backpressure. Presumably adding more
        nodes, or increasing node size in the ES cluster may reduce this pressure, but not (yet) tested.
    """

    def create_controller(self, config: dict) -> AbstractElasticsearchIndexerController:
        return PostgresElasticsearchIndexerController(config)


def parse_cli_args(options: dict, es_client) -> dict:
    passthrough_values = [
        "create_new_index",
        "drop_db_view",
        "index_name",
        "load_type",
        "partition_size",
        "process_deletes",
        "deletes_only",
        "processes",
        "skip_counts",
        "skip_delete_index",
        "skip_date_check",
    ]
    config = set_config(passthrough_values, options)

    if config["create_new_index"] and not config["index_name"]:
        raise SystemExit("Fatal error: '--create-new-index' requires '--index-name'.")
    elif config["create_new_index"]:
        config["index_name"] = config["index_name"].lower()
        config["starting_date"] = config["initial_datetime"]
        check_new_index_name_is_ok(config["index_name"], config["required_index_name"])
        if not config["skip_date_check"]:
            check_pipeline_dates(config["load_type"])
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
        config["starting_date"] != config["initial_datetime"] and not config["deletes_only"]
    )

    if config["is_incremental_load"] or config["deletes_only"]:
        if config["index_name"]:
            logger.info(format_log(f"Ignoring provided index name, using alias '{config['write_alias']}' for safety"))
        config["index_name"] = config["write_alias"]
        if not es_client.cat.aliases(name=config["write_alias"]):
            logger.error(f"Write alias '{config['write_alias']}' is missing")
            raise SystemExit(1)
    else:
        if config["index_name"] and es_client.indices.exists(config["index_name"]):
            logger.error("Data load into existing index. Change index name or run an incremental load")
            raise SystemExit(1)

    if config["starting_date"] < config["initial_datetime"]:
        logger.error(f"--start-datetime is too early. Set no earlier than {config['initial_datetime']}")
        raise SystemExit(1)

    return config


def set_config(passthrough_values: list, arg_parse_options: dict) -> dict:
    """
    Define the configurations for creating and populating each Elasticsearch index.

        Config options:
            base_table: Database table used to populate the Elasticsearch index.

            base_table_id: The `id` column of the base_table.

            create_award_type_aliases:

            data_transform_func: Python function to call to format/convert/aggregate/drop the database data before it's
                pushed to the Elasticsearch index. Can also be `None` if no data transforming needs to happen.

            data_type:

            execute_sql_func: Function that will execute the appropriate SQL to retrieve data from the database to
                populate the Elasticsearch index.

                Default value: execute_sql_statement

            extra_null_partition:

            field_for_es_id: Field that will be used as the `id` value in the Elasticsearch index.

            initial_datetime: Earliest date to return results for.
                Default value: settings.API_SEARCH_MIN_DATE  (10/01/2007)

            max_query_size: The maximum number of Elasticsearch results that can be retrieved in a single request.
                Note: Setting this too high will hurt Elasticsearch performance.
                Note: This should be defined in settings.py

                Default value: 50000

            optional_predicate: Optional SQl clause to add to the `sql_view` SQL statement.

            primary_key:

            query_alias_prefix: String used as a prefix for the Elasticsearch aliases.
                Note: This should be defined in settings.py

            required_index_name: Name of the Elasticsearch index.
                Note: This should be defined in settings.py

            sql_view: Name of the SQL View that will be used to populate the Elasticsearch index.
                Note: This should be defined in settings.py

            stored_date_key: The `name` in the `external_data_type` table to use when referencing this Elasticsearch
                index. This table, in combination with the `external_data_load_date` table, are used to keep track
                of when data was last loaded into a destination (database table or Elasticsearch index).

            unique_key_field: Name of the Elasticsearch field that will be unique across all records.

            write_alias: Name to use for the Elasticsearch alias that wil be used to write data.
                Note: This should be defined in settings.py
    """

    # Set values based on env vars and when the script started
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
            "unique_key_field": ES_AWARDS_UNIQUE_KEY_FIELD,
            "write_alias": settings.ES_AWARDS_WRITE_ALIAS,
        }
    elif arg_parse_options["load_type"] == "subaward":
        config = {
            "base_table": "subaward_search",
            "base_table_id": "broker_subaward_id",
            "create_award_type_aliases": False,
            "data_transform_func": transform_subaward_data,
            "data_type": "subaward",
            "execute_sql_func": execute_sql_statement,
            "extra_null_partition": False,
            "field_for_es_id": "broker_subaward_id",
            "initial_datetime": default_datetime,
            "max_query_size": settings.ES_SUBAWARD_MAX_RESULT_WINDOW,
            # "optional_predicate": """WHERE "update_date" >= '{starting_date}'""",
            "primary_key": "broker_subaward_id",
            "query_alias_prefix": settings.ES_SUBAWARD_QUERY_ALIAS_PREFIX,
            "required_index_name": settings.ES_SUBAWARD_NAME_SUFFIX,
            "sql_view": settings.ES_SUBAWARD_ETL_VIEW_NAME,
            # "stored_date_key": "es_awards",
            "unique_key_field": ES_SUBAWARD_UNIQUE_KEY_FIELD,
            "write_alias": settings.ES_SUBAWARD_WRITE_ALIAS,
        }
    elif arg_parse_options["load_type"] == "transaction":
        config = {
            "base_table": "vw_transaction_normalized",
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
            "unique_key_field": ES_TRANSACTIONS_UNIQUE_KEY_FIELD,
            "write_alias": settings.ES_TRANSACTIONS_WRITE_ALIAS,
        }
    elif arg_parse_options["load_type"] == "recipient":
        config = {
            "base_table": "recipient_profile",
            "base_table_id": "id",
            "create_award_type_aliases": False,
            "data_transform_func": None,
            "data_type": "recipient",
            "execute_sql_func": execute_sql_statement,
            "extra_null_partition": False,
            "field_for_es_id": "id",
            "initial_datetime": default_datetime,
            "max_query_size": settings.ES_RECIPIENTS_MAX_RESULT_WINDOW,
            "optional_predicate": "",
            "primary_key": "id",
            "query_alias_prefix": settings.ES_RECIPIENTS_QUERY_ALIAS_PREFIX,
            "required_index_name": settings.ES_RECIPIENTS_NAME_SUFFIX,
            "sql_view": settings.ES_RECIPIENTS_ETL_VIEW_NAME,
            "stored_date_key": None,
            "unique_key_field": ES_RECIPIENT_UNIQUE_KEY_FIELD,
            "write_alias": settings.ES_RECIPIENTS_WRITE_ALIAS,
        }
    elif arg_parse_options["load_type"] == "location":
        config = {
            "base_table": "transaction_search",
            "base_table_id": "transaction_id",
            "create_award_type_aliases": False,
            "data_transform_func": transform_location_data,
            "data_type": "location",
            "execute_sql_func": execute_sql_statement,
            "extra_null_partition": False,
            "field_for_es_id": "id",
            "initial_datetime": default_datetime,
            "max_query_size": settings.ES_LOCATIONS_MAX_RESULT_WINDOW,
            "optional_predicate": "",
            "primary_key": "id",
            "query_alias_prefix": settings.ES_LOCATIONS_QUERY_ALIAS_PREFIX,
            "required_index_name": settings.ES_LOCATIONS_NAME_SUFFIX,
            "sql_view": settings.ES_LOCATIONS_ETL_VIEW_NAME,
            "stored_date_key": None,
            "unique_key_field": ES_LOCATION_UNIQUE_KEY_FIELD,
            "write_alias": settings.ES_LOCATIONS_WRITE_ALIAS,
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
            "raise_status_code_3": False,
        }
    )

    return config
