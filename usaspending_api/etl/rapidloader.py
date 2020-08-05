from multiprocessing import Process, Queue
from pathlib import Path
from time import sleep

from django.conf import settings
from usaspending_api.broker.helpers.last_load_date import update_last_load_date
from usaspending_api.etl.es_etl_helpers import (
    DataJob,
    deleted_transactions,
    deleted_awards,
    download_db_records,
    es_data_loader,
    printf,
    process_guarddog,
    set_final_index_config,
    swap_aliases,
    take_snapshot,
    get_updated_record_count,
)


class Rapidloader:
    def __init__(self, config, elasticsearch_client):
        """Set values based on env vars and when the script started"""
        self.config = config
        self.elasticsearch_client = elasticsearch_client

    def run_load_steps(self) -> None:
        download_queue = Queue()  # Queue for jobs which need a csv downloaded
        es_ingest_queue = Queue(20)  # Queue for jobs which have a csv and are ready for ES ingest

        updated_record_count = get_updated_record_count(self.config)
        printf(
            {"msg": f"Found {updated_record_count:,} new {self.config['load_type']} records to add to ElasticSearch"}
        )

        job_number = 0
        for fiscal_year in self.config["fiscal_years"]:
            job_number += 1
            index = self.config["index_name"]
            filename = str(
                self.config["directory"] / "{fy}_{type}.csv".format(fy=fiscal_year, type=self.config["load_type"])
            )

            new_job = DataJob(job_number, index, fiscal_year, filename)

            if Path(filename).exists():
                Path(filename).unlink()
            download_queue.put(new_job)

        printf({"msg": "There are {} jobs to process".format(job_number)})

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
            set_final_index_config(self.elasticsearch_client, self.config["index_name"])
            if self.config["skip_delete_index"]:
                printf({"msg": "Skipping deletion of old indices"})
            else:
                printf({"msg": "Closing old indices and adding aliases"})
                swap_aliases(self.elasticsearch_client, self.config["index_name"], self.config["load_type"])

        if self.config["snapshot"]:
            printf({"msg": "Taking snapshot"})
            take_snapshot(self.elasticsearch_client, self.config["index_name"], settings.ES_REPOSITORY)

        if self.config["is_incremental_load"]:
            msg = "Storing datetime {} for next incremental load"
            printf({"msg": msg.format(self.config["processing_start_datetime"])})
            update_last_load_date("es_{}".format(self.config["load_type"]), self.config["processing_start_datetime"])
