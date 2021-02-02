import logging

from django.core.management import call_command
from math import ceil
from multiprocessing import Pool, Event, Value
from time import perf_counter
from typing import Generator, List, Tuple

from usaspending_api.broker.helpers.last_load_date import update_last_load_date
from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client
from usaspending_api.etl.elasticsearch_loader_helpers import (
    count_of_records_to_process,
    create_index,
    delete_awards,
    delete_transactions,
    extract_records,
    format_log,
    gen_random_name,
    load_data,
    obtain_extract_sql,
    set_final_index_config,
    swap_aliases,
    TaskSpec,
    toggle_refresh_on,
)
from usaspending_api.common.helpers.sql_helpers import close_all_django_db_conns

logger = logging.getLogger("script")

total_doc_success = Value("i", 0, lock=True)
total_doc_fail = Value("i", 0, lock=True)


def init_shared_abort(a: Event) -> None:
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
        self.tasks = []

    def prepare_for_etl(self) -> None:
        logger.info(format_log("Assessing data to process"))
        self.record_count, self.min_id, self.max_id = count_of_records_to_process(self.config)

        if self.record_count == 0:
            self.processes = []
            return

        self.config["partitions"] = self.determine_partitions()
        self.config["processes"] = min(self.config["processes"], self.config["partitions"])
        self.tasks = self.construct_tasks()

        logger.info(
            format_log(
                f"Created {len(self.tasks):,} task partitions"
                f" to process {self.record_count:,} total {self.config['data_type']} records"
                f" from ID {self.min_id} to {self.max_id}"
                f" with {self.config['processes']:,} parallel processes"
            )
        )

        if self.config["create_new_index"]:
            # ensure template for index is present and the latest version
            call_command("es_configure", "--template-only", f"--load-type={self.config['data_type']}")
            create_index(self.config["index_name"], instantiate_elasticsearch_client())

    def dispatch_tasks(self) -> None:
        _abort = Event()  # Event which when set signals an error occurred in a subprocess
        parallel_procs = self.config["processes"]
        with Pool(parallel_procs, maxtasksperchild=1, initializer=init_shared_abort, initargs=(_abort,)) as pool:
            pool.map(extract_transform_load, self.tasks)

        msg = f"Total documents indexed: {total_doc_success.value}, total document fails: {total_doc_fail.value}"
        logger.info(format_log(msg))

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

        close_all_django_db_conns()

        if self.config["is_incremental_load"]:
            toggle_refresh_on(client, self.config["index_name"])
            logger.info(
                format_log(f"Storing datetime {self.config['processing_start_datetime']} for next incremental load")
            )
            update_last_load_date(f"{self.config['stored_date_key']}", self.config["processing_start_datetime"])

    def determine_partitions(self) -> int:
        """Create partition size less than or equal to max_size for more even distribution"""
        if self.config["partition_size"] > (self.max_id - self.min_id):
            return 1
        # return ceil(self.record_count / self.config["partition_size"])
        return ceil(max((self.max_id - self.min_id), self.record_count) / (self.config["partition_size"]))

    def construct_tasks(self) -> List[TaskSpec]:
        """Create the Task objects w/ the appropriate configuration"""
        name_gen = gen_random_name()
        task_list = [self.configure_task(j, name_gen) for j in range(self.config["partitions"])]

        if self.config["extra_null_partition"]:
            task_list.insert(0, self.configure_task(self.config["partitions"], name_gen, True))

        return task_list

    def configure_task(self, partition_number: int, name_gen: Generator, is_null_partition: bool = False) -> TaskSpec:
        lower_bound, upper_bound = self.get_id_range_for_partition(partition_number)
        sql_config = {**self.config, **{"lower_bound": lower_bound, "upper_bound": upper_bound}}
        sql_str = obtain_extract_sql(sql_config, is_null_partition)

        return TaskSpec(
            base_table=self.config["base_table"],
            base_table_id=self.config["base_table_id"],
            execute_sql_func=self.config["execute_sql_func"],
            index=self.config["index_name"],
            is_incremental=self.config["is_incremental_load"],
            name=next(name_gen),
            partition_number=partition_number,
            primary_key=self.config["primary_key"],
            field_for_es_id=self.config["field_for_es_id"],
            sql=sql_str,
            transform_func=self.config["data_transform_func"],
            view=self.config["sql_view"],
        )

    def get_id_range_for_partition(self, partition_number: int) -> Tuple[int, int]:
        range_size = ((self.max_id - self.min_id) // self.config["partitions"]) + 1
        lower_bound = self.min_id + (range_size * partition_number)
        upper_bound = min(self.min_id + ((range_size * (partition_number + 1) - 1)), self.max_id)

        return lower_bound, upper_bound

    def run_deletes(self) -> None:
        logger.info(format_log("Processing deletions"))
        client = instantiate_elasticsearch_client()
        if self.config["data_type"] == "award":
            delete_awards(client, self.config)
        elif self.config["data_type"] == "transaction":
            delete_transactions(client, self.config)
        else:
            raise RuntimeError(f"No delete function implemented for type {self.config['data_type']}")


def extract_transform_load(task: TaskSpec) -> None:
    if abort.is_set():
        logger.warning(format_log(f"Skipping partition #{task.partition_number} due to previous error", name=task.name))
        return

    start = perf_counter()
    msg = f"Started processing on partition #{task.partition_number}: {task.name}"
    logger.info(format_log(msg, name=task.name))

    client = instantiate_elasticsearch_client()
    try:
        records = task.transform_func(task, extract_records(task))
        if abort.is_set():
            f"Prematurely ending partition #{task.partition_number} due to error in another process"
            logger.warning(format_log(msg, name=task.name))
            return
        if len(records) > 0:
            success, fail = load_data(task, records, client)
        else:
            logger.info(format_log("No records to index", name=task.name))
            success, fail = 0, 0
        with total_doc_success.get_lock():
            total_doc_success.value += success
        with total_doc_fail.get_lock():
            total_doc_fail.value += fail
    except Exception:
        if abort.is_set():
            msg = f"Partition #{task.partition_number} failed after an error was previously encountered"
            logger.warning(format_log(msg, name=task.name))
        else:
            logger.error(format_log(f"{task.name} failed!", name=task.name))
            abort.set()
    else:
        msg = f"Partition #{task.partition_number} was successfully processed in {perf_counter() - start:.2f}s"
        logger.info(format_log(msg, name=task.name))
