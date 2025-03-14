import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from psycopg2.sql import Composed
from random import choice
from typing import Any, Generator, List, Optional, Union

import psycopg2
from django.conf import settings
from elasticsearch import Elasticsearch

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
    field_for_es_id: str
    primary_key: str
    partition_number: int
    is_incremental: bool
    slices: Union[str, int]
    execute_sql_func: callable = None
    transform_func: callable = None


def chunks(items: List[Any], size: int) -> List[Any]:
    """Yield successive sized chunks from items"""
    for i in range(0, len(items), size):
        yield items[i : i + size]


def convert_json_data_to_dict(json_data: Union[dict, list, str]) -> Optional[Union[dict, List[dict]]]:
    """
    Convert provided JSON-compatible data (dict, list, or str data) into a dict so that it
    can be used as the content for an Elasticsearch nested object type.

    Do so by first getting any provided format into a format consumable by json.dumps()
    """
    if json_data is None or len(json_data) == 0:
        return None

    # Figure out what type it is, then get it into dict format
    if isinstance(json_data, dict):
        pass  # dict already
    elif isinstance(json_data, str):
        json_data = json.loads(json_data)  # try parsing as JSON if str provided
    elif isinstance(json_data, list):
        if isinstance(json_data[0], dict):
            pass  # list of dicts is ok
        elif isinstance(json_data[0], str):
            # Convert to list of dicts to make it json.dumps() consumable
            json_data = [json.loads(j) for j in json_data]
        else:
            raise ValueError(
                f"Cannot parse json_data provided as JSON (It is a list, but not a list of str or dict): {json_data}"
            )
    else:
        raise ValueError(f"Cannot parse json_data provided as JSON (not a dict, str, or list): {json_data}")

    return json_data


def dump_dict_to_string(json_data: dict[str, str] | str) -> str | None:
    """Serialize provided JSON-compatible object (dict) to a JSON formatted string so that is can be stored
    in Elasticsearch as an `object` type.

    Args:
        json_data: JSON-compatible object (dict) or string representation of a dictionary.

    Returns:
        JSON formatted string or None
    """

    if json_data is None or len(json_data) == 0:
        return None

    if isinstance(json_data, dict):
        return json.dumps(json_data)
    elif isinstance(json_data, str):
        try:
            json.loads(json_data)
            return json_data
        except ValueError as e:
            raise ValueError from e(f"String is not a valid dictionary: {json_data}")
    else:
        raise ValueError(f"Cannot serialize non-dict to string: {json_data}")


def convert_json_array_to_list_of_str(json_data: Union[list, str]) -> Optional[List[str]]:
    """
    Convert provided data (list, or str data) into a string representation of an array of JSON objects
    so that it can be used as array of values for an Elasticsearch field.

    We do this for some index fields to avoid nested types in Elasticsearch, and instead store raw stringified JSON as
    array values for a field, and deal with the JSON on the client side or with regex searches. The JSON arrays are
    converted to dictionaries to make parsing easier and then converted back into a formatted string.
    """
    if json_data is None or len(json_data) == 0:
        return None

    # Figure out what type it is, then get it into dict format
    if isinstance(json_data, str):
        json_data = json.loads(json_data)  # try parsing as JSON if str provided
        if not isinstance(json_data, list):
            raise ValueError(f"Parse str data did not yield an array of JSON objects as expected: {json_data}")
    elif isinstance(json_data, list):
        if isinstance(json_data[0], dict):
            pass  # already json.dumps() consumable (list of dicts)
        elif isinstance(json_data[0], str):
            # Convert to list of dicts to make it json.dumps() consumable
            json_data = [json.loads(j) for j in json_data]
        else:
            raise ValueError(
                f"Cannot parse json_data provided as JSON (It is a list, but not a list of str or dict): {json_data}"
            )
    else:
        raise ValueError(f"Cannot parse json_data provided as JSON (not a dict, str, or list): {json_data}")

    if json_data is None or len(json_data) == 0:  # again, in case it parsed empty
        return None
    result = []
    for j in json_data:
        result.append(json.dumps(j, sort_keys=True))
    return result


def execute_sql_statement(cmd: str | Composed, results: bool = False, verbose: bool = False) -> Optional[List[dict]]:
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
    return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def filter_query(column: str, values: list, query_type: str = "match_phrase") -> dict:
    queries = [{query_type: {column: str(i)}} for i in values]
    return {"query": {"bool": {"should": [queries]}}}


def format_log(msg: str, action: str = None, name: str = None) -> str:
    """Helper function to format log statements"""
    inner_str = f"[{action if action else 'main'}] {f'{name}' if name else ''}"
    return f"{inner_str:<34} | {msg}"


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


def is_snapshot_running(client: Elasticsearch, index_names: List[str]) -> bool:
    snapshot_list = client.snapshot.status().get("snapshots", [])
    index_names_pattern = f".*({'|'.join(index_names)}).*"
    for snapshot in snapshot_list:
        indexes = str(snapshot.get("indices", {}).keys())
        if re.match(index_names_pattern, indexes):
            return True
    return False
