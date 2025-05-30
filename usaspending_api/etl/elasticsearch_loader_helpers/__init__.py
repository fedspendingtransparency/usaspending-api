from django import setup as django_setup
from django.apps import apps

# Ensure Django is setup before importing/using ths module. Why? -->
# This module is used by other application functions, which are pickled and transported to worker machines
# in a distributed/cluster computing configuration (Spark workers). As the code lands there, no prior setup or
# dependencies are established, so setup of things like Django needs to be re-initialized before the code is run.
# Due to the way the function requires classes and functions under this module, this __init__ module is always run
# before any other entrypoint code can be invoked.
if not apps.ready:  # an indicator of whether Django has already been setup in this running process
    # NOTE: ENV VAR NAMED DJANGO_SETTINGS_MODULE must be set for setup to work (e.g. to usaspending_api.settings)
    django_setup()

from usaspending_api.etl.elasticsearch_loader_helpers.delete_data import (
    delete_awards,
    delete_docs_by_unique_key,
    delete_transactions,
)
from usaspending_api.etl.elasticsearch_loader_helpers.extract_data import (
    count_of_records_to_process,
    count_of_records_to_process_in_delta,
    extract_records,
    obtain_extract_all_partitions_sql,
    obtain_extract_partition_sql,
)
from usaspending_api.etl.elasticsearch_loader_helpers.index_config import (
    check_new_index_name_is_ok,
    check_pipeline_dates,
    create_award_type_aliases,
    create_index,
    set_final_index_config,
    swap_aliases,
    toggle_refresh_off,
    toggle_refresh_on,
)
from usaspending_api.etl.elasticsearch_loader_helpers.load_data import load_data
from usaspending_api.etl.elasticsearch_loader_helpers.transform_data import (
    transform_award_data,
    transform_location_data,
    transform_subaward_data,
    transform_transaction_data,
)
from usaspending_api.etl.elasticsearch_loader_helpers.utilities import (
    TaskSpec,
    chunks,
    execute_sql_statement,
    format_log,
    gen_random_name,
)

__all__ = [
    "chunks",
    "count_of_records_to_process",
    "count_of_records_to_process_in_delta",
    "create_award_type_aliases",
    "create_index",
    "delete_docs_by_unique_key",
    "delete_awards",
    "delete_transactions",
    "execute_sql_statement",
    "extract_records",
    "format_log",
    "gen_random_name",
    "load_data",
    "obtain_extract_partition_sql",
    "obtain_extract_all_partitions_sql",
    "set_final_index_config",
    "swap_aliases",
    "TaskSpec",
    "toggle_refresh_off",
    "toggle_refresh_on",
    "check_new_index_name_is_ok",
    "check_pipeline_dates",
    "transform_award_data",
    "transform_location_data",
    "transform_transaction_data",
    "transform_subaward_data",
]
