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
import logging

from typing import Dict

from usaspending_api.common.logging import AbbrevNamespaceUTCFormatter, ensure_logging
from usaspending_api.etl.elasticsearch_loader_helpers import TaskSpec
from usaspending_api.settings import LOGGING

logger = logging.getLogger(__name__)

# TODO: this function and all of its transient functions/dependencies needs to be made pickle-able so that it
#  can be passed into DataFrame.rdd.mapPartitionsWithIndex. Main culprit seems to be load_data(...) and what
#  it uses.
#  - an example is if any code reachable by this function -- or imported by the module this function lives in,
#  or modules that module imports -- invokes Django settings.* to access a Django setting, it will fail. This
#  is because we would be trying to use Django settings that have not yet been instantiated
#  - especially need to make sure no code from here accesses the SparkSession or SparkContext under that session
def process_partition(partition_idx: int, partition_data, task_dict: Dict[int, TaskSpec]):
    ensure_logging(logging_config_dict=LOGGING, formatter_class=AbbrevNamespaceUTCFormatter, logger_to_use=logger)
    records = [row.asDict() for row in partition_data]
    task = task_dict[partition_idx]
    # # TODO: reenable after made pickle-able
    # # success, fail = transform_load(task=task, extracted_data=records)
    logger.info(f"Would process {len(records)} records on partition #{partition_idx} with name {task.name}")
    success, fail = 0, 0
    return [(success, fail)]
