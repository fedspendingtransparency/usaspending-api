import logging

from django.core.management import call_command
from multiprocessing import Pool, Event
from random import choice
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


def init(a):
    """odd mechanism to set a global abort event in each subprocess"""
    global abort
    abort = a


class Controller:
    def __init__(self, config, elasticsearch_client):
        """Set values based on env vars and when the script started"""
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
                f"Hailing {self.config['partitions']:,} heroes"
                f" to handle {self.record_count:,} {self.config['data_type']} records"
                f" in squads of {self.config['processes']:,}"
            )
        )

        self.workers = [self.create_worker(j) for j in range(self.config["partitions"])]

        if self.config["create_new_index"]:
            # ensure template for index is present and the latest version
            call_command("es_configure", "--template-only", f"--load-type={self.config['data_type']}s")
            create_index(self.config["index_name"], instantiate_elasticsearch_client())

    def launch_workers(self):
        _abort = Event()  # Event which signals an error occured in a subprocess when set

        with Pool(self.config["processes"], maxtasksperchild=1, initializer=init, initargs=(_abort,)) as pool:
            # Using list comprehension to prevent new tasks from starting if an event occured
            pool.map(extract_transform_load, [worker for worker in self.workers if not _abort.is_set()])

        if _abort.is_set():
            raise RuntimeError("One or more heroes have fallen! Initiate strategic retreat")

    def complete_process(self) -> None:
        if self.config["create_new_index"]:
            client = instantiate_elasticsearch_client()
            set_final_index_config(client, self.config["index_name"])
            if self.config["skip_delete_index"]:
                logger.info(format_log("Skipping deletion of old indices"))
            else:
                logger.info(format_log("Closing old indices and adding aliases"))
                swap_aliases(client, self.config)

        if self.config["is_incremental_load"]:
            toggle_refresh_on(self.elasticsearch_client, self.config["index_name"])
            logger.info(
                format_log(f"Storing datetime {self.config['processing_start_datetime']} for next incremental load")
            )
            update_last_load_date(f"{self.config['stored_date_key']}", self.config["processing_start_datetime"])

    def create_worker(self, number: int) -> WorkerNode:
        sql_str = EXTRACT_SQL.format(
            divisor=self.config["partitions"],
            id_col=self.config["primary_key"],
            remainder=number,
            update_date=self.config["starting_date"],
            view=self.config["sql_view"],
        )
        name_gen = gen_random_name()

        return WorkerNode(
            index=self.config["index_name"],
            primary_key=self.config["primary_key"],
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
        else:
            deleted_transactions(client, self.config)


def extract_transform_load(worker):
    if abort.is_set():
        logger.info(format_log(f"{worker.name} became lost on the way over."))
        return

    start = perf_counter()
    a = choice(["skillfully", "deftly", "expertly", "readily", "quickly", "nimbly", "casually", "easily", "boldly"])
    logger.info(format_log(f"{worker.name.upper()} {a} enters the arena", job=worker.name))

    client = instantiate_elasticsearch_client()
    try:
        records = worker.transform_func(worker, extract_records(worker))
        load_data(worker, records, client)
    except Exception:
        if abort.is_set():
            logger.info(format_log(f"{worker.name} realizes the battle lost. #ragequit"))
        else:
            msg = f"{worker.name} has fallen in battle! How could such a thing happen to this great warrior?"
            logger.error(msg, job=worker.name)
            abort.set()
    else:
        attrib = choice(["pro", "champ", "boss", "top dog", "hero", "super"])
        msg = f"{worker.name} completed the mission like a {attrib} in {perf_counter() - start:.2f}s"
        logger.info(format_log(msg, job=worker.name))
