import logging

from django.conf import settings
from multiprocessing import Process, Queue
from pathlib import Path
from time import sleep
from typing import Tuple

from usaspending_api.broker.helpers.last_load_date import update_last_load_date
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
    take_snapshot,
    get_updated_record_count,
    toggle_refresh_on,
)

logger = logging.getLogger("script")


class Controller:
    def __init__(self, config, elasticsearch_client):
        """Set values based on env vars and when the script started"""
        self.config = config
        self.elasticsearch_client = elasticsearch_client

    def run_load_steps(self) -> None:
        download_queue = Queue()  # Queue for jobs which need a csv downloaded
        es_ingest_queue = Queue(20)  # Queue for jobs which have a csv and are ready for ES ingest

        updated_record_count = get_updated_record_count(self.config)
        logger.info(format_log(f"Found {updated_record_count:,} {self.config['load_type']} records to index"))

        if updated_record_count == 0:
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

        if updated_record_count != 0:  # only run if there are data to process
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

        if updated_record_count != 0:
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

        if self.config["snapshot"]:
            logger.info(format_log("Taking snapshot"))
            take_snapshot(self.elasticsearch_client, self.config["index_name"], settings.ES_REPOSITORY)

        if self.config["is_incremental_load"]:
            toggle_refresh_on(self.elasticsearch_client, self.config["index_name"])
            logger.info(
                format_log(f"Storing datetime {self.config['processing_start_datetime']} for next incremental load")
            )
            update_last_load_date(f"es_{self.config['load_type']}", self.config["processing_start_datetime"])
