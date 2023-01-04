"""
WARNING WARNING WARNING
!!!!!!!!!!!!!!!!!!!!!!!
This module must be managed very carefully and stay lean.

The main functions: copy_csv_from_s3_to_pg and copy_csvs_from_s3_to_pg
are used in distributed/parallel/multiprocess execution (by Spark) and is
pickled via cloudpickle. As such it must not have any presumed setup code that would have run (like Django setup,
logging configuration, etc.) and must encapsulate all of those dependencies (like logging config) on its own.

Adding new imports to this module may inadvertently introduce a dependency that can't be pickled.

As it stands, even if new imports are added to the modules it already imports, it could lead to a problem.
"""
from django import setup as django_setup
# NOTE: ENV VAR NAMED DJANGO_SETTINGS_MODULE must be set for setup to work (e.g. to usaspending_api.settings)
django_setup()

import logging
import os
from time import perf_counter

from typing import Dict, List, Tuple

from django import setup as django_setup

from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client
from usaspending_api.common.logging import AbbrevNamespaceUTCFormatter, ensure_logging
from usaspending_api.config import CONFIG
#from usaspending_api.etl.elasticsearch_loader_helpers.utilities import TaskSpec, format_log
from usaspending_api.etl.elasticsearch_loader_helpers import TaskSpec
from usaspending_api.settings import LOGGING

logger = logging.getLogger(__name__)


def show_partition_data(partition_idx: int, partition_data):
    """Dummy RDD-mappable function that works without pickle/dependency-initialization errors"""
    print(f"Hello from lambda partition#{partition_idx}")
    records = [row.asDict() for row in partition_data]
    record_count = len(records)
    print(f"Showing 2 records of {record_count} for partition #{partition_idx}")
    print(records[0])
    print(records[1])
    return [(record_count, 0)]

def init_django():
    # Set Runtime Env
    os.environ["ENV_CODE"] = "qat"

    # Setup Django
    os.environ["DJANGO_SETTINGS_MODULE"] = "usaspending_api.settings"
    django_setup()


# TODO: this function and all of its transient functions/dependencies needs to be made pickle-able so that it
#  can be passed into DataFrame.rdd.mapPartitionsWithIndex. Main culprit seems to be load_data(...) and what
#  it uses.
#  - an example is if any code reachable by this function -- or imported by the module this function lives in,
#  or modules that module imports -- invokes Django settings.* to access a Django setting, it will fail. This
#  is because we would be trying to use Django settings that have not yet been instantiated
#  - especially need to make sure no code from here accesses the SparkSession or SparkContext under that session
def process_partition(partition_idx: int, partition_data, task_name): #TaskSpec):
    #django_setup()
    ensure_logging(logging_config_dict=LOGGING, formatter_class=AbbrevNamespaceUTCFormatter, logger_to_use=logger)
    logger.info(f"Hello from process_partition. Processing partition#{partition_idx}")
    print(f"Hello from process_partition. Processing partition#{partition_idx}")
    records = [row.asDict() for row in partition_data]
    records_len = len(records)
    logger.info(f"{records_len} records to process on partition#{partition_idx}")
    print(f"{records_len} records to process on partition#{partition_idx}")
    #task_name = task.name
    #task = task_dict[partition_idx]
    logger.info(f"Task {task_name} processing data on partition#{partition_idx}")
    print(f"Task {task_name} processing data on partition#{partition_idx}")
    # TODO: reenable after made pickle-able
    #success, fail = transform_load(task=task, extracted_data=records)
    logger.info(f"Would process {records_len} records on partition #{partition_idx} with name {task_name}")
    print(f"Would process {records_len} records on partition #{partition_idx} with name {task_name}")
    success, fail = records_len, 0
    return [(success, fail)]


def transform_load(task, extracted_data: List[Dict]) -> Tuple[int, int]:
    #     if abort.is_set():
    #         logger.warning(format_log(f"Skipping partition #{task.partition_number} due to previous error", name=task.name))
    #         return

    start = perf_counter()
    msg = f"Started processing on partition #{task.partition_number}: {task.name}"
    logger.info(format_log(msg, name=task.name))

    client = instantiate_elasticsearch_client(CONFIG.ES_URL)
    try:
        # extracted_data = extract_records(task)
        #records = task.transform_func(task, extracted_data)
        # TODO: renable task.transform_func after made pickle-able
        logger.info("SKIPPING transform_func for testing purposes")
        records = []
        #         if abort.is_set():
        #             f"Prematurely ending partition #{task.partition_number} due to error in another process"
        #             logger.warning(format_log(msg, name=task.name))
        #             return
        if len(records) > 0:
            # TODO: renable load_data once pickle-able
            logger.info("SKIPPING load_data for testing purposes")
            # success, fail = load_data(task, records, client)
            success, fail = 0, 0
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


def format_log(msg: str, action: str = None, name: str = None) -> str:
    """Helper function to format log statements"""
    inner_str = f"[{action if action else 'main'}] {f'{name}' if name else ''}"
    return f"{inner_str:<34} | {msg}"
