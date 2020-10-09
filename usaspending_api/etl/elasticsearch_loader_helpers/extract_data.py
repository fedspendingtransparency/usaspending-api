import logging

from time import perf_counter
from usaspending_api.etl.elasticsearch_loader_helpers.utilities import format_log, execute_sql_statement

logger = logging.getLogger("script")

COUNT_SQL = """
SELECT COUNT(*) AS count
FROM "{sql_view}"
WHERE "update_date" >= '{starting_date}'
"""

EXTRACT_SQL = """
    SELECT *
    FROM "{sql_view}"
    WHERE "update_date" >= '{starting_date}' AND mod("{primary_key}", {divisor}) = {remainder}
"""


def obtain_count_sql(config: dict) -> str:
    return COUNT_SQL.format(**config)


def obtain_extract_sql(config: dict) -> str:
    return EXTRACT_SQL.format(**config)


def count_of_records_to_process(config: dict) -> int:
    start = perf_counter()
    count = execute_sql_statement(obtain_count_sql(config), True, config["verbose"])[0]["count"]
    msg = f"Found {count:,} {config['data_type']} DB records, took {perf_counter() - start:.2f}s"
    logger.info(format_log(msg, action="Extract"))
    return count


def extract_records(task):
    start = perf_counter()
    logger.info(format_log(f"Extracting data from source", name=task.name, action="Extract"))

    try:
        records = execute_sql_statement(task.sql, True)
    except Exception as e:
        logger.exception(f"Failed on partition {task.name} with '{task.sql}'")
        raise e

    msg = f"{len(records):,} records extracted in {perf_counter() - start:.2f}s"
    logger.info(format_log(msg, name=task.name, action="Extract"))
    return records
