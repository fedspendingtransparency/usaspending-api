import os
import json

from datetime import datetime, timezone
from django.core.management.base import BaseCommand
from elasticsearch import Elasticsearch
from multiprocessing import Process, Queue
from time import perf_counter
from time import sleep

from usaspending_api import settings
from usaspending_api.broker.helpers.last_load_date import get_last_load_date
from usaspending_api.broker.helpers.last_load_date import update_last_load_date
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


# SCRIPT OBJECTIVES and ORDER OF EXECUTION STEPS
# 1. Generate the full list of fiscal years and award descriptions to process as jobs
# 2. Iterate by job
#   a. Download 1 CSV file by year and trans type
#       i. Download the next CSV file until no more jobs need CSVs
#   b. Upload CSV to Elasticsearch
# 3. Take a snapshot of the index reloaded
#
# IF RELOADING ---
# [command] --index_name=NEWINDEX --swap --snapshot

ES = Elasticsearch(settings.ES_HOSTNAME, timeout=300)


class Command(BaseCommand):
    help = """"""

    # used by parent class
    def add_arguments(self, parser):
        parser.add_argument("fiscal_years", nargs="+", type=str, metavar="fiscal-years")
        parser.add_argument(
            "--start-datetime",
            type=datetime_command_line_argument_type(naive=False),
            help="Processes transactions updated on or after the UTC date/time "
            "provided. yyyy-mm-dd hh:mm:ss is always a safe format. Wrap in "
            "quotes if date/time contains spaces."
        )
        parser.add_argument(
            "--dir",
            default=os.path.dirname(os.path.abspath(__file__)),
            type=str,
            help="Set for a custom location of output files",
        )
        parser.add_argument(
            "--index-name", type=str, help="Set the index name to index new data", required=True
        )
        parser.add_argument("-d", "--deleted", action="store_true", help="Flag to include deleted transactions from S3")
        parser.add_argument(
            "-w",
            "--swap",
            action="store_true",
            help="Flag allowed to put aliases to index and close all indices with aliases associated",
        )
        parser.add_argument(
            "-s", "--snapshot", action="store_true", help="Take a snapshot of the current cluster and save to S3"
        )

    # used by parent class
    def handle(self, *args, **options):
        """ Script execution of custom code starts in this method"""
        start = perf_counter()
        processing_start_datetime = datetime.now(timezone.utc)
        printf({"msg": "Starting script\n{}".format("=" * 56)})

        self.config = set_config()
        self.config["verbose"] = True if options["verbosity"] > 1 else False
        if "all" in options["fiscal_years"]:
            self.config["fiscal_years"] = create_fiscal_year_list(start_year=2008)
        else:
            self.config["fiscal_years"] = [int(x) for x in options["fiscal_years"]]
        self.config["directory"] = options["dir"] + os.sep
        self.config["provide_deleted"] = options["deleted"]
        self.config["swap"] = options["swap"]
        self.config["snapshot"] = options["snapshot"]
        self.config["index_name"] = options["index_name"].lower()

        mappingfile = os.path.join(settings.BASE_DIR, "usaspending_api/etl/es_transaction_mapping.json")
        with open(mappingfile) as f:
            mapping_dict = json.load(f)
            self.config["mapping"] = json.dumps(mapping_dict)
        self.config["doc_type"] = str(list(mapping_dict["mappings"].keys())[0])
        self.config["max_query_size"] = mapping_dict["settings"]["index.max_result_window"]

        does_index_exist = ES.indices.exists(self.config["index_name"])
        is_incremental_load = False

        if not does_index_exist:
            msg = '"{}" does not exist, skipping deletions for ths load, provide_deleted overwritten to False'
            printf({"msg": msg.format(self.config["index_name"])})
            self.config["provide_deleted"] = False

        default_datetime = datetime.strptime("2007-10-01+0000", "%Y-%m-%d%z")

        if options["start_datetime"]:
            self.config["starting_date"] = options["start_datetime"]
            is_incremental_load = True
        else:
            # Due to the queries used for fetching postgres data, `starting_date` needs to be present and a date
            #   before the earliest records in S3 and when Postgres records were updated.
            #   Choose the beginning of FY2008, and make it timezone-award for S3
            self.config["starting_date"] = get_last_load_date("es_transactions", default=default_datetime)

        is_incremental_load = self.config["starting_date"] != default_datetime

        if not os.path.isdir(self.config["directory"]):
            printf({"msg": "Provided directory does not exist"})
            raise SystemExit(1)
        elif self.config["starting_date"] < default_datetime:
            printf({"msg": "`start-datetime` is too early. Set to after {}".format(default_datetime)})
            raise SystemExit(1)
        elif does_index_exist and not is_incremental_load:
            printf({"msg": "Full data load into existing index! Change destination index or load a subset of data"})
            raise SystemExit(1)

        start_msg = "target index: {index_name} | FY(s): {fiscal_years} | Starting from: {starting_date}"
        printf({"msg": start_msg.format(**self.config)})

        self.controller()

        if is_incremental_load:
            printf({"msg": "Updating Last Load record with {}".format(processing_start_datetime)})
            update_last_load_date("es_transactions", processing_start_datetime)
        printf({"msg": "---------------------------------------------------------------"})
        printf({"msg": "Script completed in {} seconds".format(perf_counter() - start)})
        printf({"msg": "---------------------------------------------------------------"})

    def controller(self):

        download_queue = Queue()  # Queue for jobs whch need a csv downloaded
        es_ingest_queue = Queue(20)  # Queue for jobs which have a csv and are ready for ES ingest

        job_number = 0
        for fy in self.config["fiscal_years"]:
            job_number += 1
            index = self.config["index_name"]
            filename = "{dir}{fy}_transactions.csv".format(
                dir=self.config["directory"], fy=fy
            )

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
                name="ES Index Process", target=es_data_loader, args=(ES, download_queue, es_ingest_queue, self.config)
            )
        )

        process_list[0].start()  # Start Download process

        if self.config["provide_deleted"]:
            process_list.append(
                Process(name="S3 Deleted Records Scrapper Process", target=deleted_transactions, args=(ES, self.config))
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

        if self.config["swap"]:
            printf({"msg": "Closing old indices and adding aliases"})
            swap_aliases(ES, self.config["index_name"])

        if self.config["snapshot"]:
            printf({"msg": "Taking snapshot"})
            take_snapshot(ES, self.config["index_name"], settings.ES_REPOSITORY)


def set_config():
    return {
        "aws_region": settings.USASPENDING_AWS_REGION,
        "s3_bucket": settings.DELETED_TRANSACTIONS_S3_BUCKET_NAME,
        "root_index": settings.TRANSACTIONS_INDEX_ROOT,
        "formatted_now": datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),  # ISO8601
    }
