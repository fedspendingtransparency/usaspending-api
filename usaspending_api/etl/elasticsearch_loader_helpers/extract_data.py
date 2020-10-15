import logging

from time import perf_counter
from usaspending_api.etl.elasticsearch_loader_helpers.utilities import format_log, execute_sql_statement

logger = logging.getLogger("script")

COUNT_SQL = """
    SELECT COUNT(*) AS count
    FROM "{sql_view}"
    {optional_predicate}
""".replace(
    "\n", ""
)

EXTRACT_SQL = """
    SELECT *
    FROM "{sql_view}"
    {optional_predicate} "{primary_key}" BETWEEN {lower_bound} AND {upper_bound}
""".replace(
    "\n", ""
)

MIN_MAX_COUNT_SQL = """
    SELECT min({primary_key}) AS min, max({primary_key}) AS max, count(*) AS count
    FROM "{sql_view}"
    {optional_predicate}
""".replace(
    "\n", ""
)


def obtain_count_sql(config: dict) -> str:
    if "optional_predicate" not in config:
        config["optional_predicate"] = ""
    return COUNT_SQL.format(**config).format(**config)  # fugly. Allow string values to have expressions


def obtain_min_max_count_sql(config: dict) -> str:
    if "optional_predicate" not in config:
        config["optional_predicate"] = ""
    sql = MIN_MAX_COUNT_SQL.format(**config).format(**config)  # fugly. Allow string values to have expressions
    return sql


def obtain_extract_sql(config: dict) -> str:
    if not config.get("optional_predicate"):
        config["optional_predicate"] = "WHERE"
    else:
        config["optional_predicate"] += " AND "
    return EXTRACT_SQL.format(**config).format(**config)  # fugly. Allow string values to have expressions


def count_of_records_to_process(config: dict) -> int:
    start = perf_counter()
    results = execute_sql_statement(obtain_min_max_count_sql(config), True, config["verbose"])[0]
    min_id, max_id, count = results["min"], results["max"], results["count"]
    msg = f"Found {count:,} {config['data_type']} DB records, took {perf_counter() - start:.2f}s"
    logger.info(format_log(msg, action="Extract"))
    return count, min_id, max_id


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
