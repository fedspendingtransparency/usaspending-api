import json
import logging
import psycopg2

from dataclasses import dataclass
from django.conf import settings
from pathlib import Path
from random import choice
from typing import Any, Generator, List, Optional


from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string

logger = logging.getLogger("script")


@dataclass
class TaskSpec:
    """Contains details for a single ETL task"""

    name: str
    index: str
    sql: str
    view: str
    base_table: str
    base_table_id: str
    primary_key: str
    partition_number: int
    is_incremental: bool
    transform_func: callable = None


def chunks(items: List[Any], size: int) -> List[Any]:
    """Yield successive sized chunks from items"""
    for i in range(0, len(items), size):
        yield items[i : i + size]


def convert_postgres_json_array_to_list(json_array: dict) -> Optional[List]:
    """
        Postgres JSON arrays (jsonb) are stored in CSVs as strings. Since we want to avoid nested types
        in Elasticsearch the JSON arrays are converted to dictionaries to make parsing easier and then
        converted back into a formatted string.
    """
    if json_array is None or len(json_array) == 0:
        return None
    result = []
    for j in json_array:
        for key, value in j.items():
            j[key] = "" if value is None else str(j[key])
        result.append(json.dumps(j, sort_keys=True))
    return result


def execute_sql_statement(cmd: str, results: bool = False, verbose: bool = False) -> Optional[List[dict]]:
    """Simple function to execute SQL using a single-use psycopg2 connection"""
    rows = None
    if verbose:
        print(cmd)

    with psycopg2.connect(dsn=get_database_dsn_string()) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(cmd)
            if results:
                rows = db_rows_to_dict(cursor)
    return rows


def db_rows_to_dict(cursor: psycopg2.extensions.cursor) -> List[dict]:
    """Return a dictionary of all row results from a database connection cursor"""
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def filter_query(column: str, values: list, query_type: str = "match_phrase") -> dict:
    queries = [{query_type: {column: str(i)}} for i in values]
    return {"query": {"bool": {"should": [queries]}}}


def format_log(msg: str, action: str = None, name: str = None) -> str:
    """Helper function to format log statements"""
    inner_str = f"[{action if action else 'main'}] {f'{name}' if name else ''}"
    return f"{inner_str:<32} | {msg}"


def gen_random_name() -> Generator[str, None, None]:
    """
        Generate over 5000 unique names in a random order.
        Successive calls past the unique name combinations will infinitely
        continue to generate additional unique names w/ roman numerals appended.
    """
    name_dict = json.loads(Path(settings.APP_DIR / "data" / "multiprocessing_worker_names.json").read_text())
    attributes = list(set(name_dict["attributes"]))
    subjects = list(set(name_dict["subjects"]))
    upper_limit = len(attributes) * len(subjects)
    name_str = "{attribute} {subject}{loop}"
    previous_names, full_cycles, loop = [], 0, ""

    def to_roman_numerals(num: int) -> str:
        return (
            ("I" * num)
            .replace("IIIII", "V")
            .replace("IIII", "IV")
            .replace("VV", "X")
            .replace("VIV", "IX")
            .replace("XXXXX", "L")
            .replace("XXXX", "XL")
            .replace("LL", "C")
            .replace("LXL", "XC")
            .replace("CCCCC", "D")
            .replace("CCCC", "CD")
            .replace("DD", "M")
            .replace("DCD", "CM")
        )

    while True:
        random_a, random_s = choice(attributes), choice(subjects)
        name = name_str.format(attribute=random_a, subject=random_s, loop=loop)

        if name not in previous_names:
            previous_names.append(name)
            yield name

        if len(previous_names) >= (upper_limit + (upper_limit * full_cycles)):
            full_cycles += 1
            loop = f" {to_roman_numerals(full_cycles)}"
