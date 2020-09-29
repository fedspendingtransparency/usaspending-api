import logging

from django.conf import settings
from time import perf_counter

from usaspending_api.etl.elasticsearch_loader_helpers.utilities import format_log, execute_sql_statement

logger = logging.getLogger("script")

COUNT_SQL = """
SELECT COUNT(*) AS count
FROM "{view}"
WHERE "update_date" >= '{update_date}'
"""

EXTRACT_SQL = """
    SELECT *
    FROM "{view}"
    WHERE "update_date" >= '{update_date}' AND "{id_col}" % {divisor} = {remainder}
"""

# ==============================================================================
# Other Globals
# ==============================================================================

AWARD_DESC_CATEGORIES = {
    "loans": "loans",
    "grant": "grants",
    "insurance": "other",
    "other": "other",
    "contract": "contracts",
    "direct payment": "directpayments",
}

UNIVERSAL_TRANSACTION_ID_NAME = "generated_unique_transaction_id"
UNIVERSAL_AWARD_ID_NAME = "generated_unique_award_id"


def count_of_records_to_process(config, sort_ids=False):
    if config["load_type"] == "awards":
        view_name = settings.ES_AWARDS_ETL_VIEW_NAME
    else:
        view_name = settings.ES_TRANSACTIONS_ETL_VIEW_NAME

    count_sql = COUNT_SQL.format(update_date=config["starting_date"], view=view_name)

    return execute_sql_statement(count_sql, True, config["verbose"])[0]["count"]


def extract_records(worker):
    start = perf_counter()
    try:
        records = execute_sql_statement(worker.sql, True)
    except Exception as e:
        logger.exception(f"Worker {worker.name} failed with '{worker.sql}'")
        raise e

    logger.info(
        format_log(
            f"{len(records):,} records extracted in {perf_counter() - start:.2f}s", job=worker.name, process="Download"
        )
    )
    return records
