from multiprocessing import Process, Queue
from pathlib import Path
from time import sleep

from usaspending_api import settings
from usaspending_api.broker.helpers.last_load_date import update_last_load_date
from usaspending_api.etl.es_etl_helpers import (
    DataJob,
    download_db_records,
    es_data_loader,
    printf,
    process_guarddog,
    set_final_index_config,
    swap_aliases,
    take_snapshot,
)


class AwardRapidloader:
    def __init__(self, *args):
        self.config = args[0]
        self.config["awards"] = True
        self.elasticsearch_client = args[1]

    def run_load_steps(self) -> None:
        download_queue = Queue()  # Queue for jobs whch need a csv downloaded
        es_ingest_queue = Queue(20)  # Queue for jobs which have a csv and are ready for ES ingest

        job_number = 0
        for fiscal_year in self.config["fiscal_years"]:
            job_number += 1
            index = self.config["index_name"]
            filename = str(self.config["directory"] / "{fy}_awards.csv".format(fy=fiscal_year))

            new_job = DataJob(job_number, index, fiscal_year, filename)

            if Path(filename).exists():
                Path(filename).unlink()
            download_queue.put(new_job)

        printf({"msg": "There are {} jobs to process".format(job_number)})

        process_list = []
        process_list.append(
            Process(
                name="Download Process",
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
            print(self.config["index_name"])
            swap_aliases(self.elasticsearch_client, self.config["index_name"], True)

        if self.config["snapshot"]:
            printf({"msg": "Taking snapshot"})
            take_snapshot(self.elasticsearch_client, self.config["index_name"], settings.ES_REPOSITORY)

        if self.config["is_incremental_load"]:
            msg = "Storing datetime {} for next incremental load"
            printf({"msg": msg.format(self.config["processing_start_datetime"])})
            update_last_load_date("es_awards", self.config["processing_start_datetime"])
