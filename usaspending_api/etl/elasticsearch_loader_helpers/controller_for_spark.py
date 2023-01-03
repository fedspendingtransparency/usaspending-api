import logging

from typing import List, Dict, Tuple, Generator
from time import perf_counter
from math import ceil

from django.core.management import call_command
from pyspark.sql import SparkSession
from django.conf import settings

from usaspending_api.broker.helpers.last_load_date import update_last_load_date
from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client
from usaspending_api.common.helpers.sql_helpers import close_all_django_db_conns
from usaspending_api.common.logging import AbbrevNamespaceUTCFormatter, ensure_logging
from usaspending_api.config import CONFIG
from usaspending_api.etl.spark_transform_and_load import show_partition_data
from usaspending_api.settings import LOGGING
from usaspending_api.etl.elasticsearch_loader_helpers.index_config import (
    toggle_refresh_on,
    swap_aliases,
    set_final_index_config,
    create_index,
)
from usaspending_api.etl.elasticsearch_loader_helpers.utilities import TaskSpec, format_log, gen_random_name
from usaspending_api.etl.elasticsearch_loader_helpers.extract_data import obtain_min_max_count_sql

# from usaspending_api.etl.elasticsearch_loader_helpers.load_data import load_data

logger = logging.getLogger("inc_load_es_index_from_delta")
ensure_logging(logging_config_dict=LOGGING, formatter_class=AbbrevNamespaceUTCFormatter, logger_to_use=logger)


def ensure_view_exists(view_name: str, spark: SparkSession) -> None:
    # Taking advantage of this running in a spark notebook to get SparkSession spark variable
    view_file_path = settings.APP_DIR / "database_scripts" / "etl" / f"{view_name}.sql"

    view_sql = view_file_path.read_text()
    temp_view_select_sql = view_sql.replace(f"DROP VIEW IF EXISTS {view_name};", "")
    temp_view_select_sql = temp_view_select_sql.replace("CREATE VIEW", "CREATE OR REPLACE TEMP VIEW")
    temp_view_select_sql = temp_view_select_sql.replace("::JSON", "::string")
    temp_view_select_sql = temp_view_select_sql.replace('"', "")

    spark.sql(
        f"""
    {temp_view_select_sql}
    """
    )


def transform_load(task: TaskSpec, extracted_data: List[Dict]) -> Tuple[int, int]:
    #     if abort.is_set():
    #         logger.warning(format_log(f"Skipping partition #{task.partition_number} due to previous error", name=task.name))
    #         return

    start = perf_counter()
    msg = f"Started processing on partition #{task.partition_number}: {task.name}"
    logger.info(format_log(msg, name=task.name))

    client = instantiate_elasticsearch_client(CONFIG.ES_URL)
    try:
        # extracted_data = extract_records(task)
        records = task.transform_func(task, extracted_data)
        #         if abort.is_set():
        #             f"Prematurely ending partition #{task.partition_number} due to error in another process"
        #             logger.warning(format_log(msg, name=task.name))
        #             return
        if len(records) > 0:
            # TODO: renable load_data once pickle-able
            logger.info("SKIPPING load_data for testing purposes")
            # success, fail = load_data(task, records, client)
            success, fail = 0
        else:
            logger.info(format_log("No records to index", name=task.name))
            success, fail = 0, 0
    #         with total_doc_success.get_lock():
    #             total_doc_success.value += success
    #         with total_doc_fail.get_lock():
    #             total_doc_fail.value += fail
    except Exception as exc:
        #         if abort.is_set():
        #             msg = f"Partition #{task.partition_number} failed after an error was previously encountered"
        #             logger.warning(format_log(msg, name=task.name))
        #         else:
        logger.exception(format_log(f"{task.name} failed!", name=task.name), exc)
        raise exc
    #             abort.set()

    else:
        msg = f"Partition #{task.partition_number} was successfully processed in {perf_counter() - start:.2f}s"
        logger.info(format_log(msg, name=task.name))
    return success, fail


def count_of_records_to_process_delta(config: dict, spark: SparkSession) -> Tuple[int, int, int]:
    start = perf_counter()
    results = spark.sql(obtain_min_max_count_sql(config).replace('"', "")).collect()[0].asDict()
    min_id, max_id, count = results["min"], results["max"], results["count"]
    msg = f"Found {count:,} {config['data_type']} DB records, took {perf_counter() - start:.2f}s"
    logger.info(format_log(msg, action="Extract"))
    return count, min_id, max_id


# NOTE: Removed double quotes around identifiers, as Spark SQL does not like it
EXTRACT_ALL_NON_NULL_PARTITIONS_SQL = """
    SELECT *
    FROM {sql_view}
    {optional_predicate} {primary_key} IS NOT NULL
""".replace(
    "\n", ""
)


def obtain_non_null_partitions_sql(config: Dict):
    if not config.get("optional_predicate"):
        config["optional_predicate"] = "WHERE"
    else:
        config["optional_predicate"] += " AND "

    sql = EXTRACT_ALL_NON_NULL_PARTITIONS_SQL
    return sql.format(**config).format(**config)  # fugly. Allow string values to have expressions


class Controller:
    """Controller for multiprocess Elasticsearch ETL"""

    def __init__(self, config, spark: SparkSession):
        self.config = config
        self.spark = spark
        self.tasks = {}

    def prepare_for_etl(self) -> None:
        logger.info(format_log("Assessing data to process"))
        self.record_count, self.min_id, self.max_id = count_of_records_to_process_delta(self.config, self.spark)

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
        extract_sql = obtain_non_null_partitions_sql(self.config)
        extract_sql = extract_sql.replace('"', "")  # Spark SQL does not process quoted identifiers correctly
        print(f"Using extract_sql:\n{extract_sql}")
        df = self.spark.sql(extract_sql)
        # lookback_date = '2022-10-30' # ~692k records
        # df = df.where(f"etl_update_date > '{lookback_date}'")
        df_record_count = df.count()
        print(
            f"Repartitioning {df_record_count} records from {df.rdd.getNumPartitions()} partitions into "
            f"{self.config['partitions']} partitions to evenly balance no more than {self.config['partition_size']} "
            f"records per partition. Then handing each partition to available executors for processing."
        )
        df = df.repartition(self.config["partitions"])

        # Must have a clean/detached copy of this dict, with no self ref, whose class has a reference of the
        # SparkContext. SparkContext references CANNOT be pickled with cloudpickle
        task_dict = {**self.tasks}
        # def show_data(partition_idx: int, partition_data):
        #     print(f"Hello from lambda partition#{partition_idx}")
        #     records = [row.asDict() for row in partition_data]
        #     record_count = len(records)
        #     print(f"Showing 2 records of {record_count} for partition #{partition_idx}")
        #     print(records[0])
        #     print(records[1])
        #     return [(record_count, 0)]
        success_fail_stats = df.rdd.mapPartitionsWithIndex(
            lambda partition_idx, partition_data: show_partition_data(partition_idx, partition_data),
            # lambda partition_idx, partition_data: process_partition(
            #     partition_idx=partition_idx,
            #     partition_data=partition_data,
            #     task_dict=task_dict,
            # ),
            preservesPartitioning=True,
        ).collect()

        successes, failures = 0, 0
        for sf in success_fail_stats:
            successes += sf[0]
            failures += sf[1]
        msg = f"Total documents indexed: {successes}, total document fails: {failures}"
        logger.info(format_log(msg))

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
        """Simple strategy to divide total record count by ideal partition size"""
        #         """Simple strategy of partitions that cover the id-range in an even distribution"""
        #         id_range_item_count = self.max_id - self.min_id + 1  # total number or records if all IDs exist in DB
        #         if self.config["partition_size"] > id_range_item_count:
        #             return 1
        #         return ceil(id_range_item_count / self.config["partition_size"])
        return ceil(self.record_count / self.config["partition_size"])

    def construct_tasks(self) -> Dict[int, TaskSpec]:
        """Create the Task objects w/ the appropriate configuration"""
        name_gen = gen_random_name()
        task_offset = 1 if self.config["extra_null_partition"] else 0
        task_dict = {
            j + task_offset: self.configure_task(j + task_offset, name_gen) for j in range(self.config["partitions"])
        }
        if self.config["extra_null_partition"]:
            task_dict[0] = self.configure_task(self.config["partitions"], name_gen, True)

        return task_dict

    def configure_task(self, partition_number: int, name_gen: Generator, is_null_partition: bool = False) -> TaskSpec:
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
            # sql=sql_str,
            sql=None,
            transform_func=self.config["data_transform_func"],
            view=self.config["sql_view"],
        )

    def get_id_range_for_partition(self, partition_number: int) -> Tuple[int, int]:
        raise NotImplementedError

    #         partition_size = self.config["partition_size"]
    #         lower_bound = self.min_id + (partition_number * partition_size)
    #         upper_bound = min(lower_bound + partition_size - 1, self.max_id)
    #         return lower_bound, upper_bound

    def run_deletes(self) -> None:
        raise NotImplementedError


#         logger.info(format_log("Processing deletions"))
#         client = instantiate_elasticsearch_client()
#         if self.config["data_type"] == "award":
#             delete_awards(client, self.config)
#         elif self.config["data_type"] == "transaction":
#             delete_transactions(client, self.config)
#             # Use the lesser of the fabs/fpds load dates as the es_deletes load date. This
#             # ensures all records deleted since either job was run are taken into account
#             last_db_delete_time = get_earliest_load_date(["fabs", "fpds"])
#             update_last_load_date("es_deletes", last_db_delete_time)
#         else:
#             raise RuntimeError(f"No delete function implemented for type {self.config['data_type']}")
