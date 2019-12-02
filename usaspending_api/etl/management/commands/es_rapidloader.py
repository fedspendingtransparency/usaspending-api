from datetime import datetime, timezone
from django.core.management.base import BaseCommand
from multiprocessing import Process, Queue
from pathlib import Path
from time import perf_counter, sleep

from usaspending_api import settings
from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date
from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client
from usaspending_api.common.elasticsearch.elasticsearch_sql_helpers import ensure_transaction_etl_view_exists
from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type, fy as parse_fiscal_year
from usaspending_api.common.helpers.fiscal_year_helpers import create_fiscal_year_list
from usaspending_api.etl.es_etl_helpers import (
    DataJob,
    deleted_transactions,
    download_db_records,
    es_data_loader,
    printf,
    process_guarddog,
    set_final_index_config,
    swap_aliases,
    take_snapshot,
)


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

    def handle(self, *args, **options):
        self.elasticsearch_client = instantiate_elasticsearch_client()
        self.config = process_cli_parameters(options, self.elasticsearch_client)

        start = perf_counter()
        printf({"msg": "Starting script\n{}".format("=" * 56)})
        start_msg = "target index: {index_name} | FY(s): {fiscal_years} | Starting from: {starting_date}"
        printf({"msg": start_msg.format(**self.config)})
        ensure_transaction_etl_view_exists()

        self.run_load_steps()
        self.complete_process()

        printf({"msg": "---------------------------------------------------------------"})
        printf({"msg": "Script completed in {} seconds".format(perf_counter() - start)})
        printf({"msg": "---------------------------------------------------------------"})

    def run_load_steps(self) -> None:

        download_queue = Queue()  # Queue for jobs whch need a csv downloaded
        es_ingest_queue = Queue(20)  # Queue for jobs which have a csv and are ready for ES ingest

        job_number = 0
        for fiscal_year in self.config["fiscal_years"]:
            job_number += 1
            index = self.config["index_name"]
            filename = str(self.config["directory"] / "{fy}_transactions.csv".format(fy=fiscal_year))

            new_job = DataJob(job_number, index, fiscal_year, filename)

            if Path(filename).exists():
                Path(filename).unlink()
            download_queue.put(new_job)

        printf({"msg": "There are {} jobs to process".format(job_number)})

        process_list = []
        process_list.append(
            Process(
                name="Download Proccess",
                target=download_db_records,
                args=(download_queue, es_ingest_queue, self.config),
            )
        )
        process_list.append(
            Process(
                name="ES Index Process",
                target=es_data_loader,
                args=(self.elasticsearch_client, download_queue, es_ingest_queue, self.config),
            )
        )

        process_list[0].start()  # Start Download process

        if self.config["process_deletes"]:
            process_list.append(
                Process(
                    name="S3 Deleted Records Scrapper Process",
                    target=deleted_transactions,
                    args=(self.elasticsearch_client, self.config),
                )
            )
            process_list[-1].start()  # start S3 csv fetch proces
            while process_list[-1].is_alive():
                printf({"msg": "Waiting to start ES ingest until S3 deletes are complete"})
                sleep(7)

        process_list[1].start()  # start ES ingest process

        while True:
            sleep(10)
            if process_guarddog(process_list):
                raise SystemExit("Fatal error: review logs to determine why process died.")
            elif all([not x.is_alive() for x in process_list]):
                printf({"msg": "All ETL processes completed execution with no error codes"})
                break

    def complete_process(self) -> None:
        if self.config["create_new_index"]:
            printf({"msg": "Closing old indices and adding aliases"})
            set_final_index_config(self.elasticsearch_client, self.config["index_name"])
            swap_aliases(self.elasticsearch_client, self.config["index_name"])

        if self.config["snapshot"]:
            printf({"msg": "Taking snapshot"})
            take_snapshot(self.elasticsearch_client, self.config["index_name"], settings.ES_REPOSITORY)

        if self.config["is_incremental_load"]:
            msg = "Storing datetime {} for next incremental load"
            printf({"msg": msg.format(self.config["processing_start_datetime"])})
            update_last_load_date("es_transactions", self.config["processing_start_datetime"])


def process_cli_parameters(options: dict, es_client) -> None:
    default_datetime = datetime.strptime("{}+0000".format(settings.API_SEARCH_MIN_DATE), "%Y-%m-%d%z")
    simple_args = ("process_deletes", "create_new_index", "snapshot", "index_name", "directory", "skip_counts")
    config = set_config(simple_args, options)

    config["fiscal_years"] = fiscal_years_for_processing(options)
    config["directory"] = Path(config["directory"]).resolve()

    if config["create_new_index"] and not config["index_name"]:
        raise SystemExit("Fatal error: --create-new-index requires --index-name.")
    elif config["create_new_index"]:
        config["index_name"] = config["index_name"].lower()
        config["starting_date"] = default_datetime
        check_new_index_name_is_ok(config["index_name"])
    elif options["start_datetime"]:
        config["starting_date"] = options["start_datetime"]
    else:
        # Due to the queries used for fetching postgres data,
        #  `starting_date` needs to be present and a date before:
        #      - The earliest records in S3.
        #      - When all transaction records in the USAspending SQL database were updated.
        #   And keep it timezone-award for S3
        config["starting_date"] = get_last_load_date("es_transactions", default=default_datetime)

    config["max_query_size"] = settings.ES_TRANSACTIONS_MAX_RESULT_WINDOW

    config["is_incremental_load"] = not bool(config["create_new_index"]) and (
        config["starting_date"] != default_datetime
    )

    if config["is_incremental_load"]:
        if config["index_name"]:
            msg = "Ignoring provided index name, using alias '{}' for incremental load"
            printf({"msg": msg.format(settings.ES_TRANSACTIONS_WRITE_ALIAS)})
        config["index_name"] = settings.ES_TRANSACTIONS_WRITE_ALIAS
        if not es_client.cat.aliases(name=settings.ES_TRANSACTIONS_WRITE_ALIAS):
            printf({"msg": "Fatal error: write alias '{}' is missing".format(settings.ES_TRANSACTIONS_WRITE_ALIAS)})
            raise SystemExit(1)
    else:
        if es_client.indices.exists(config["index_name"]):
            printf({"msg": "Fatal error: data load into existing index. Change index name or run an incremental load"})
            raise SystemExit(1)

    if not config["directory"].is_dir():
        printf({"msg": "Fatal error: provided directory does not exist"})
        raise SystemExit(1)
    elif config["starting_date"] < default_datetime:
        printf({"msg": "Fatal error: --start-datetime is too early. Set no earlier than {}".format(default_datetime)})
        raise SystemExit(1)
    elif not config["is_incremental_load"] and config["process_deletes"]:
        printf({"msg": "Skipping deletions for ths load, --deleted overwritten to False"})
        config["process_deletes"] = False

    return config


def set_config(copy_args: list, arg_parse_options: dict) -> dict:
    """Set values based on env vars and when the script started"""
    config = {
        "aws_region": settings.USASPENDING_AWS_REGION,
        "s3_bucket": settings.DELETED_TRANSACTIONS_S3_BUCKET_NAME,
        "root_index": settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX,
        "processing_start_datetime": datetime.now(timezone.utc),
        "verbose": arg_parse_options["verbosity"] > 1,  # convert the management command's levels of verbosity to a bool
    }

    config.update({k: v for k, v in arg_parse_options.items() if k in copy_args})
    return config


def fiscal_years_for_processing(options: list) -> list:
    if "all" in options["fiscal_years"]:
        return create_fiscal_year_list(start_year=parse_fiscal_year(settings.API_SEARCH_MIN_DATE))
    return [int(x) for x in options["fiscal_years"]]


def check_new_index_name_is_ok(provided_name: str) -> None:
    if not provided_name.endswith(settings.ES_TRANSACTIONS_NAME_SUFFIX):
        raise SystemExit(
            "new index name doesn't end with the expected pattern: '{}'".format(settings.ES_TRANSACTIONS_NAME_SUFFIX)
        )
