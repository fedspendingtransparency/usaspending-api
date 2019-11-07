import os

from datetime import datetime, timezone
from django.core.management.base import BaseCommand
from multiprocessing import Process
from multiprocessing import Queue
from time import perf_counter
from time import sleep

from usaspending_api import settings
from usaspending_api.broker.helpers.last_load_date import get_last_load_date
from usaspending_api.broker.helpers.last_load_date import update_last_load_date
from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client
from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type
from usaspending_api.common.helpers.fiscal_year_helpers import create_fiscal_year_list
from usaspending_api.etl.es_etl_helpers import DataJob
from usaspending_api.etl.es_etl_helpers import deleted_transactions
from usaspending_api.etl.es_etl_helpers import download_db_records
from usaspending_api.etl.es_etl_helpers import es_data_loader
from usaspending_api.etl.es_etl_helpers import printf
from usaspending_api.etl.es_etl_helpers import process_guarddog
from usaspending_api.etl.es_etl_helpers import swap_aliases
from usaspending_api.etl.es_etl_helpers import take_snapshot


class Command(BaseCommand):
    help = """ETL script for Elasticsearch index of transaction data
        PROCESS
             1. Generate the full list of fiscal years to process as jobs
             2. Iterate by job
               a. Download a CSV file by year (one at a time)
                   i. Continue to download a CSV file until all years are downloaded
               b. Upload a CSV to Elasticsearch
                   i. Continue to upload a CSV file until all years are uploaded to ES
               c. Delete CSV file
        IF RELOADING ---
            python3 manage.py es_rapidloader --index-name NEWINDEX --reload-all all
    """

    default_datetime = datetime.strptime("{}+0000".format(settings.API_SEARCH_MIN_DATE), "%Y-%m-%d%z")

    def add_arguments(self, parser):
        parser.add_argument(
            "fiscal_years",
            nargs="+",
            type=str,
            metavar="fiscal-years",
            help="Provide a list of fiscal years to process. For convenience, provide 'all' for FY2008 to current FY",
        )
        parser.add_argument(
            "--deleted",
            action="store_true",
            help="When this flag is set, the script will include the process to "
            "obtain records of deleted transactions from S3 and remove from the index",
            dest="provide_deleted",
        )
        parser.add_argument(
            "--dir",
            default=os.path.dirname(os.path.abspath(__file__)),
            type=str,
            help="Set for a custom location of output files",
            dest="directory",
        )
        parser.add_argument(
            "--fast",
            action="store_true",
            help="When this flag is set, the ETL process will skip the record counts to reduce operation time",
        )
        parser.add_argument(
            "--index-name",
            type=str,
            help="Provide the target index which will be indexed with the new data and process deletes (if --deleted)",
        )
        parser.add_argument(
            "--reload-all",
            action="store_true",
            help="Load all transactions. It needs a new unique index name "
            "and set aliases used by API logic to the new index",
        )
        parser.add_argument(
            "--snapshot",
            action="store_true",
            help="Create a new Elasticsearch snapshot of the current index state which is stored in S3",
        )
        parser.add_argument(
            "--start-datetime",
            type=datetime_command_line_argument_type(naive=False),
            help="Processes transactions updated on or after the UTC date/time "
            "provided. yyyy-mm-dd hh:mm:ss is always a safe format. Wrap in "
            "quotes if date/time contains spaces.",
        )

    # used by parent class
    def handle(self, *args, **options):
        """ Script execution of custom code starts in this method"""
        self.elasticsearch_client = instantiate_elasticsearch_client()
        self.transform_cli_arguments(options)

        start = perf_counter()
        printf({"msg": "Starting script\n{}".format("=" * 56)})
        start_msg = "target index: {index_name} | FY(s): {fiscal_years} | Starting from: {starting_date}"
        printf({"msg": start_msg.format(**self.config)})

        self.controller()

        if self.config["is_incremental_load"]:
            printf({"msg": "Updating Last Load record with {}".format(self.config["processing_start_datetime"])})
            update_last_load_date("es_transactions", self.config["processing_start_datetime"])
        printf({"msg": "---------------------------------------------------------------"})
        printf({"msg": "Script completed in {} seconds".format(perf_counter() - start)})
        printf({"msg": "---------------------------------------------------------------"})

    def transform_cli_arguments(self, options):
        simple_args = ("provide_deleted", "reload_all", "snapshot", "index_name", "directory", "fast")
        self.config = set_config(simple_args, options)

        self.config["fiscal_years"] = fiscal_years_for_processing(options)
        self.config["directory"] = self.config["directory"] + os.sep

        if self.config["reload_all"] and not self.config["index_name"]:
            raise SystemExit("--reload-all requires an index name in --index-name")
        elif self.config["reload_all"]:
            self.config["index_name"] = self.config["index_name"].lower()

            if not self.config["index_name"].endswith(settings.ES_TRANSACTIONS_NAME_PATTERN):
                raise SystemExit("new index name doesn't end with the expected pattern: '{}'".format(settings.ES_TRANSACTIONS_NAME_PATTERN))
            self.config["starting_date"] = self.default_datetime
        elif options["start_datetime"]:
            self.config["index_name"] = settings.ES_TRANSACTIONS_WRITE_ALIAS
            self.config["starting_date"] = options["start_datetime"]
        else:
            # Due to the queries used for fetching postgres data,
            #  `starting_date` needs to be present and a date before:
            #      - The earliest records in S3.
            #      - When all transaction records in the USAspending SQL database were updated.
            #   And keep it timezone-award for S3
            self.config["starting_date"] = get_last_load_date("es_transactions", default=self.default_datetime)

        self.config["max_query_size"] = settings.ES_TRANSACTIONS_MAX_RESULT_WINDOW

        does_index_exist = self.elasticsearch_client.indices.exists(self.config["index_name"])
        self.config["is_incremental_load"] = (self.config["starting_date"] != self.default_datetime) or not bool(
            self.config["reload_all"]
        )

        if not os.path.isdir(self.config["directory"]):
            printf({"msg": "Provided directory does not exist"})
            raise SystemExit(1)
        elif self.config["starting_date"] < self.default_datetime:
            printf({"msg": "`start-datetime` is too early. Set to after {}".format(self.default_datetime)})
            raise SystemExit(1)
        elif does_index_exist and not self.config["is_incremental_load"]:
            printf({"msg": "Full data load into existing index! Change destination index or load a subset of data"})
            raise SystemExit(1)
        elif not does_index_exist or self.config["reload_all"]:
            printf({"msg": "Skipping deletions for ths load, provide_deleted overwritten to False"})
            self.config["provide_deleted"] = False

    def controller(self):

        download_queue = Queue()  # Queue for jobs whch need a csv downloaded
        es_ingest_queue = Queue(20)  # Queue for jobs which have a csv and are ready for ES ingest

        job_number = 0
        for fy in self.config["fiscal_years"]:
            job_number += 1
            index = self.config["index_name"]
            filename = "{dir}{fy}_transactions.csv".format(dir=self.config["directory"], fy=fy)

            new_job = DataJob(job_number, index, fy, filename)

            if os.path.exists(filename):
                os.remove(filename)
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

        if self.config["provide_deleted"]:
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
                raise SystemExit(1)
            elif all([not x.is_alive() for x in process_list]):
                printf({"msg": "All ETL processes completed execution with no error codes"})
                break

        if self.config["reload_all"]:
            printf({"msg": "Closing old indices and adding aliases"})
            swap_aliases(self.elasticsearch_client, self.config["index_name"])

        if self.config["snapshot"]:
            printf({"msg": "Taking snapshot"})
            take_snapshot(self.elasticsearch_client, self.config["index_name"], settings.ES_REPOSITORY)


def set_config(copy_args, arg_parse_options):
    # Set values based on env vars and when the script started
    config = {
        "aws_region": settings.USASPENDING_AWS_REGION,
        "s3_bucket": settings.DELETED_TRANSACTIONS_S3_BUCKET_NAME,
        "root_index": settings.ES_TRANSACTIONS_READ_ALIAS_PREFIX,
        "processing_start_datetime": datetime.now(timezone.utc),
    }

    # convert the management command's levels of verbosity to a boolean
    config["verbose"] = arg_parse_options["verbosity"] > 1

    # simple 1-to-1 transfer from argParse to internal config dict
    for arg in copy_args:
        config[arg] = arg_parse_options[arg]
    return config


def fiscal_years_for_processing(options):
    if options["reload_all"] or "all" in options["fiscal_years"]:
        return create_fiscal_year_list(start_year=2008)
    return [int(x) for x in options["fiscal_years"]]
