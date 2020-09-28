import logging

from django.conf import settings
from django.core.management import call_command
from math import ceil
from multiprocessing import Process, Queue, Pool
from pathlib import Path
from random import choice
from time import sleep
from typing import Tuple

from usaspending_api.broker.helpers.last_load_date import update_last_load_date
from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client
from usaspending_api.etl.elasticsearch_loader_helpers import (
    DataJob,
    deleted_transactions,
    deleted_awards,
    download_db_records,
    es_data_loader,
    format_log,
    process_guarddog,
    set_final_index_config,
    swap_aliases,
    obtain_all_ids_to_process,
    WorkerNode,
    gen_random_name,
    EXTRACT_SQL,
    chunks,
    extract_records,
    transform_data,
    load_data,
)

logger = logging.getLogger("script")


class Controller:
    def __init__(self, config, elasticsearch_client):
        """Set values based on env vars and when the script started"""
        self.config = config
        self.elasticsearch_client = elasticsearch_client

    def prepare_for_etl(self):
        logger.info(format_log("Assessing data to process"))
        id_list = obtain_all_ids_to_process(self.config, True)
        self.updated_record_count = len(id_list)
        self.fractions = self.partion_ids(id_list, self.config["batch_size"])

        self.config["workers"] = min(self.config["workers"], len(self.fractions))

        logger.info(
            format_log(
                f"Created {len(self.fractions):,} jobs to process"
                f" {self.updated_record_count:,} {self.config['load_type']} records"
                f" using {self.config['workers']:,} workers"
            )
        )

        self.workers = [self.create_worker(f) for f in self.fractions]

        if self.config["create_new_index"]:
            # ensure template for index is present and the latest version
            call_command("es_configure", "--template-only", f"--load-type={self.config['load_type']}")

    def extract(self):
        with Pool(self.config["workers"]) as pool:
            pool.map(self.extract_transform_load, self.workers)

    @staticmethod
    def extract_transform_load(worker):
        random_verb = choice(
            ["skillfully", "deftly", "expertly", "readily", "resourcefully", "nimbly", "agilely", "casually", "easily"]
        )
        logger.info(
            format_log(
                f"{worker.name.upper()} enters the arena to {random_verb} handle {len(worker.ids)} docs",
                job=worker.name,
            )
        )

        client = instantiate_elasticsearch_client()
        records = extract_records(worker)
        records = transform_data(worker, records)
        load_data(worker, records, client)

    def run_load_steps(self) -> None:
        download_queue = Queue()  # Queue for jobs which need a csv downloaded
        es_ingest_queue = Queue(20)  # Queue for jobs which have a csv and are ready for ES ingest

        if self.updated_record_count == 0:
            jobs = 0
        else:
            download_queue, jobs = self.create_download_jobs()

        logger.info(format_log(f"There are {jobs} jobs to process"))

        process_list = [
            Process(
                name="Download Process",
                target=download_db_records,
                args=(download_queue, es_ingest_queue, self.config),
            ),
            Process(
                name="ES Index Process",
                target=es_data_loader,
                args=(self.elasticsearch_client, download_queue, es_ingest_queue, self.config),
            ),
        ]

        if self.updated_record_count != 0:  # only run if there are data to process
            process_list[0].start()  # Start Download process

        if self.config["process_deletes"]:
            process_list.append(
                Process(
                    name="S3 Deleted Records Scrapper Process",
                    target=deleted_transactions if self.config["load_type"] == "transactions" else deleted_awards,
                    args=(self.elasticsearch_client, self.config),
                )
            )
            process_list[-1].start()  # start S3 csv fetch proces
            while process_list[-1].is_alive():
                logger.info(format_log("Waiting to start ES ingest until S3 deletes are complete"))
                sleep(7)  # add a brief pause to make sure the deletes are processed in ES

        if self.updated_record_count != 0:
            process_list[1].start()  # start ES ingest process

        while True:
            sleep(10)
            if process_guarddog(process_list):
                raise SystemExit("Fatal error: review logs to determine why process died.")
            elif all([not x.is_alive() for x in process_list]):
                logger.info(format_log("All ETL processes completed execution with no error codes"))
                break

    def create_download_jobs(self) -> Tuple[Queue, int]:
        download_queue = Queue()
        for job_number, fiscal_year in enumerate(self.config["fiscal_years"], start=1):
            index = self.config["index_name"]
            filename = str(self.config["directory"] / f"{fiscal_year}_{self.config['load_type']}.csv")

            new_job = DataJob(job_number, index, fiscal_year, filename)

            if Path(filename).exists():
                Path(filename).unlink()
            download_queue.put(new_job)
        return download_queue, job_number

    def complete_process(self) -> None:
        if self.config["create_new_index"]:
            set_final_index_config(self.elasticsearch_client, self.config["index_name"])
            if self.config["skip_delete_index"]:
                logger.info(format_log("Skipping deletion of old indices"))
            else:
                logger.info(format_log("Closing old indices and adding aliases"))
                swap_aliases(self.elasticsearch_client, self.config["index_name"], self.config["load_type"])

        if self.config["is_incremental_load"]:
            logger.info(
                format_log(f"Storing datetime {self.config['processing_start_datetime']} for next incremental load")
            )
            update_last_load_date(f"es_{self.config['load_type']}", self.config["processing_start_datetime"])

    @staticmethod
    def partion_ids(id_list, max_size):
        """Create N partions of IDs with max size of max_size"""
        size = ceil(len(id_list) / ceil(len(id_list) / max_size))
        return list([c for c in chunks(id_list, size)])

    def create_worker(self, fraction):
        if self.config["load_type"] == "awards":
            id_col = "award_id"
            view = settings.ES_AWARDS_ETL_VIEW_NAME
            transform_func = None
        else:
            id_col = "transaction_id"
            view = settings.ES_TRANSACTIONS_ETL_VIEW_NAME
            transform_func = None

        return WorkerNode(
            ids=fraction,
            index=self.config["index_name"],
            load_type=self.config["load_type"],
            name=next(gen_random_name()),
            sql=EXTRACT_SQL.format(id_col=id_col, view=view, ids=tuple(fraction)),
            transform_func=transform_func,
        )
