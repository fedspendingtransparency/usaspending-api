import logging
from abc import ABC, abstractmethod
from math import ceil
from multiprocessing import Event, Pool, Value
from time import perf_counter
from typing import Dict, Tuple

from django.core.management import call_command

from usaspending_api.broker.helpers.last_load_date import get_earliest_load_date, update_last_load_date
from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client
from usaspending_api.common.elasticsearch.elasticsearch_sql_helpers import ensure_view_exists
from usaspending_api.common.helpers.sql_helpers import close_all_django_db_conns
from usaspending_api.etl.elasticsearch_loader_helpers import (
    TaskSpec,
    count_of_records_to_process,
    create_index,
    delete_awards,
    delete_transactions,
    extract_records,
    format_log,
    gen_random_name,
    load_data,
    obtain_extract_partition_sql,
    set_final_index_config,
    swap_aliases,
    toggle_refresh_on,
)

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


class AbstractElasticsearchIndexerController(ABC):
    def __init__(self, config):
        self.config = config
        self.tasks = {}

    @abstractmethod
    def ensure_view_exists(self, sql_view_name: str, force_recreate=True) -> None:
        pass

    @abstractmethod
    def prepare_for_etl(self) -> None:
        logger.info(format_log("Assessing data to process"))
        self.record_count, self.min_id, self.max_id = self._count_of_records_to_process(self.config)

        if self.record_count == 0:
            # We should always have at least one partition
            self.config["partitions"] = 1
            self.config["processes"] = 0
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

    @abstractmethod
    def determine_partitions(self) -> int:
        pass

    def construct_tasks(self) -> Dict[int, TaskSpec]:
        """Create the Task objects w/ the appropriate configuration"""
        name_gen = gen_random_name()
        task_offset = 1 if self.config["extra_null_partition"] else 0
        task_dict = {
            j + task_offset: self.configure_task(j + task_offset, next(name_gen))
            for j in range(self.config["partitions"])
        }
        if self.config["extra_null_partition"]:
            task_dict[0] = self.configure_task(0, next(name_gen), True)

        return task_dict

    @abstractmethod
    def configure_task(self, partition_number: int, task_name: str, is_null_partition: bool = False) -> TaskSpec:
        pass

    def _construct_task_spec(
        self,
        partition_number,
        task_name,
        extract_sql_str,
    ):
        return TaskSpec(
            base_table=self.config["base_table"],
            base_table_id=self.config["base_table_id"],
            execute_sql_func=self.config["execute_sql_func"],
            index=self.config["index_name"],
            is_incremental=self.config["is_incremental_load"],
            name=task_name,
            partition_number=partition_number,
            primary_key=self.config["primary_key"],
            field_for_es_id=self.config["field_for_es_id"],
            sql=extract_sql_str,
            transform_func=self.config["data_transform_func"],
            view=self.config["sql_view"],
            slices=self.config["slices"],
        )

    def set_slice_count(self) -> None:
        """
        Retrieves the number of slices that should be used when performing any type
        of scroll operation (e.g., delete_by_query).
        """
        if self.config["create_new_index"] or self.config["load_type"] != "transaction":
            # Only transactions are currently processing with more than 5 shards. As a result,
            # all other load types are set to the original default of "auto".
            slice_count = "auto"
        else:
            slice_count = 1
        self.config["slices"] = slice_count
        logger.info(
            format_log(f"Setting the value of {self.config['load_type']} index slices: {self.config['slices']}")
        )

    @abstractmethod
    def get_id_range_for_partition(self, partition_number: int) -> Tuple[int, int]:
        pass

    @abstractmethod
    def dispatch_tasks(self) -> None:
        pass

    def run_deletes(self) -> None:
        logger.info(format_log("Processing deletions"))
        if self.config["data_type"] == "award":
            self._run_award_deletes()
        elif self.config["data_type"] == "transaction":
            self._run_transaction_deletes()
        else:
            raise RuntimeError(f"No delete function implemented for type {self.config['data_type']}")

    @abstractmethod
    def _run_award_deletes(self):
        pass

    @abstractmethod
    def _run_transaction_deletes(self):
        pass

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

    @abstractmethod
    def cleanup(self) -> None:
        """Method that can be overridden for any final cleanup. If nothing to cleanup, implement with ``pass``"""
        pass

    @abstractmethod
    def _count_of_records_to_process(self, config) -> Tuple[int, int, int]:
        pass


class PostgresElasticsearchIndexerController(AbstractElasticsearchIndexerController):
    """Controller for multiprocess Elasticsearch ETL that extracts data from a Postgres database"""

    def ensure_view_exists(self, sql_view_name: str, force_recreate=True) -> None:
        ensure_view_exists(view_name=sql_view_name, force=force_recreate)

    def _count_of_records_to_process(self, config) -> Tuple[int, int, int]:
        return count_of_records_to_process(self.config)

    def determine_partitions(self) -> int:
        """Simple strategy of partitions that cover the id-range in an even distribution"""
        id_range_item_count = self.max_id - self.min_id + 1  # total number or records if all IDs exist in DB
        if self.config["partition_size"] > id_range_item_count:
            return 1
        return ceil(id_range_item_count / self.config["partition_size"])

    def get_id_range_for_partition(self, partition_number: int) -> Tuple[int, int]:
        partition_size = self.config["partition_size"]
        lower_bound = self.min_id + (partition_number * partition_size)
        upper_bound = min(lower_bound + partition_size - 1, self.max_id)
        return lower_bound, upper_bound

    def prepare_for_etl(self) -> None:
        super().prepare_for_etl()

    def configure_task(self, partition_number: int, task_name: str, is_null_partition: bool = False) -> TaskSpec:
        lower_bound, upper_bound = self.get_id_range_for_partition(partition_number)
        sql_config = {**self.config, **{"lower_bound": lower_bound, "upper_bound": upper_bound}}
        sql_str = obtain_extract_partition_sql(sql_config, is_null_partition)

        return self._construct_task_spec(partition_number, task_name, sql_str)

    def dispatch_tasks(self) -> None:
        _abort = Event()  # Event which when set signals an error occurred in a subprocess
        parallel_procs = self.config["processes"]
        with Pool(parallel_procs, maxtasksperchild=1, initializer=init_shared_abort, initargs=(_abort,)) as pool:
            pool.map(extract_transform_load, self.tasks.values())

        msg = f"Total documents indexed: {total_doc_success.value}, total document fails: {total_doc_fail.value}"
        logger.info(format_log(msg))

        if _abort.is_set():
            raise RuntimeError("One or more partitions failed!")

    def _run_award_deletes(self):
        client = instantiate_elasticsearch_client()
        delete_awards(
            client=client,
            config=self.config,
            fabs_external_data_load_date_key="transaction_fabs",
            fpds_external_data_load_date_key="transaction_fpds",
        )

    def _run_transaction_deletes(self):
        client = instantiate_elasticsearch_client()
        delete_transactions(
            client=client,
            config=self.config,
            fabs_external_data_load_date_key="transaction_fabs",
            fpds_external_data_load_date_key="transaction_fpds",
        )
        # Use the lesser of the fabs/fpds load dates as the es_deletes load date. This
        # ensures all records deleted since either job was run are taken into account
        last_db_delete_time = get_earliest_load_date(
            ["transaction_fabs", "transaction_fpds"], format_func=(lambda log_msg: format_log(log_msg, action="Delete"))
        )
        update_last_load_date("es_deletes", last_db_delete_time)
        logger.info(format_log(f"Updating `es_deletes`: {last_db_delete_time}", action="Delete"))

    def cleanup(self) -> None:
        pass


def extract_transform_load(task: TaskSpec) -> None:
    if abort.is_set():
        logger.warning(format_log(f"Skipping partition #{task.partition_number} due to previous error", name=task.name))
        return

    start = perf_counter()
    msg = f"Started processing on partition #{task.partition_number}: {task.name}"
    logger.info(format_log(msg, name=task.name))

    client = instantiate_elasticsearch_client()
    try:
        if task.transform_func is not None:
            records = task.transform_func(task, extract_records(task))
        else:
            records = extract_records(task)
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
            logger.exception(format_log(f"{task.name} failed!", name=task.name))
            abort.set()
    else:
        msg = f"Partition #{task.partition_number} was successfully processed in {perf_counter() - start:.2f}s"
        logger.info(format_log(msg, name=task.name))
