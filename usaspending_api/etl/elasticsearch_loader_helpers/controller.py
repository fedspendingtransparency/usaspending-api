import logging

from django.core.management import call_command
from multiprocessing import Pool, Event
from time import perf_counter

from usaspending_api.broker.helpers.last_load_date import update_last_load_date
from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client
from usaspending_api.etl.elasticsearch_loader_helpers import (
    count_of_records_to_process,
    create_index,
    deleted_awards,
    deleted_transactions,
    extract_records,
    EXTRACT_SQL,
    format_log,
    gen_random_name,
    load_data,
    set_final_index_config,
    swap_aliases,
    toggle_refresh_on,
    WorkerNode,
)

logger = logging.getLogger("script")


def init_shared_abort(a):
    """
        Odd mechanism to set a global abort event in each subprocess
        Inspired by https://stackoverflow.com/a/59984671
    """
    global abort
    abort = a


class Controller:
    """Controller for multiprocess Elasticsearch ETL"""

    def __init__(self, config):
        self.config = config

    def prepare_for_etl(self):
        if self.config["process_deletes"]:
            self.run_deletes()
        logger.info(format_log("Assessing data to process"))
        self.record_count = count_of_records_to_process(self.config)

        if self.record_count == 0:
            self.processes = []
            return

        self.config["processes"] = min(self.config["processes"], self.config["partitions"])

        logger.info(
            format_log(
                f"Created {self.config['partitions']:,} partitions"
                f" to process {self.record_count:,} total {self.config['data_type']} records"
                f" with {self.config['processes']:,} parellel processes"
            )
        )

        self.workers = self.create_workers()

        if self.config["create_new_index"]:
            # ensure template for index is present and the latest version
            call_command("es_configure", "--template-only", f"--load-type={self.config['data_type']}s")
            create_index(self.config["index_name"], instantiate_elasticsearch_client())

    def launch_workers(self):
        _abort = Event()  # Event which when set signals an error occured in a subprocess
        parellel_procs = self.config["processes"]
        with Pool(parellel_procs, maxtasksperchild=1, initializer=init_shared_abort, initargs=(_abort,)) as pool:
            pool.map(extract_transform_load, self.workers)

        if _abort.is_set():
            raise RuntimeError("One or more partitions failed!")

    def complete_process(self) -> None:
        client = instantiate_elasticsearch_client()
        if self.config["create_new_index"]:
            set_final_index_config(client, self.config["index_name"])
            if self.config["skip_delete_index"]:
                logger.info(format_log("Skipping deletion of old indices"))
            else:
                logger.info(format_log("Closing old indices and adding aliases"))
                swap_aliases(client, self.config)

        if self.config["is_incremental_load"]:
            toggle_refresh_on(client, self.config["index_name"])
            logger.info(
                format_log(f"Storing datetime {self.config['processing_start_datetime']} for next incremental load")
            )
            update_last_load_date(f"{self.config['stored_date_key']}", self.config["processing_start_datetime"])

    def create_workers(self):

        name_gen = gen_random_name()
        return [self.create_worker(j, name_gen) for j in range(self.config["partitions"])]

    def create_worker(self, number: int, name_gen) -> WorkerNode:
        sql_str = EXTRACT_SQL.format(
            divisor=self.config["partitions"],
            id_col=self.config["primary_key"],
            remainder=number,
            update_date=self.config["starting_date"],
            view=self.config["sql_view"],
        )

        return WorkerNode(
            index=self.config["index_name"],
            primary_key=self.config["primary_key"],
            partition_number=number,
            name=next(name_gen),
            sql=sql_str,
            is_incremental=self.config["is_incremental_load"],
            transform_func=self.config["data_transform_func"],
        )

    def run_deletes(self):
        logger.info(format_log("Processing deletions"))
        client = instantiate_elasticsearch_client()
        if self.config["data_type"] == "award":
            deleted_awards(client, self.config)
        elif self.config["data_type"] == "transaction":
            deleted_transactions(client, self.config)
        else:
            raise RuntimeError(f"No delete function implemented for type {self.config['data_type']}")


def extract_transform_load(worker):
    if abort.is_set():
        logger.info(format_log(f"{worker.name} partition was skipped due to previous error"))
        return

    start = perf_counter()
    msg = f"Started processing on partition #{worker.partition_number}: {worker.name}"
    logger.info(format_log(msg, job=worker.name))

    client = instantiate_elasticsearch_client()
    try:
        records = worker.transform_func(worker, extract_records(worker))
        if abort.is_set():
            logger.info(format_log(f"Prematurely ending {worker.name} due to previous error"))
            return
        load_data(worker, records, client)
    except Exception:
        if abort.is_set():
            logger.info(format_log(f"{worker.name} failed after an error was previously encountered"))
        else:
            logger.error(format_log(f"{worker.name} failed!", job=worker.name))
            abort.set()
    else:
        msg = f"Partition {worker.name} was successfully processed in {perf_counter() - start:.2f}s"
        logger.info(format_log(msg, job=worker.name))
