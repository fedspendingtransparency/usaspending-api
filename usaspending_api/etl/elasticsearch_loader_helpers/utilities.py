import json
import logging

import psycopg2

from dataclasses import dataclass
from django.conf import settings
from pathlib import Path
from random import choice
from typing import Any, Generator, List, Optional, Union

from pandas import Series

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
    execute_sql_func: callable = None
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


def create_agg_key(record: Union[dict, Series], key_name: str):
    """ Returns the json.dumps() for an Ordered Dictionary representing the agg_key """

    def _recipient_agg_key():
        if record["recipient_hash"] is None or record["recipient_levels"] is None:
            return {
                "name": record["recipient_name"],
                "unique_id": record["recipient_unique_id"],
                "hash": None,
                "levels": None,
            }
        return {
            "name": record["recipient_name"],
            "unique_id": record["recipient_unique_id"],
            "hash": str(record["recipient_hash"]),
            "levels": record["recipient_levels"],
        }

    def _agency_agg_key(agency_type, agency_tier):
        if record[f"{agency_type}_{agency_tier}_agency_name"] is None:
            return None
        result = {"name": record[f"{agency_type}_{agency_tier}_agency_name"]}
        if f"{agency_type}_{agency_tier}_agency_abbreviation" in record:
            result["abbreviation"] = record[f"{agency_type}_{agency_tier}_agency_abbreviation"]
        if f"{agency_type}_{agency_tier}_agency_code" in record:
            result["code"] = record[f"{agency_type}_{agency_tier}_agency_code"]
        result["id"] = record[f"{agency_type}_{agency_tier + '_' if agency_tier == 'toptier' else ''}agency_id"]
        return result

    def _naics_agg_key():
        if record["naics_code"] is None:
            return None
        return {"code": record["naics_code"], "description": record["naics_description"]}

    def _psc_agg_key():
        if record["product_or_service_code"] is None:
            return None
        return {"code": record["product_or_service_code"], "description": record["product_or_service_description"]}

    def _county_agg_key(location_type):
        if record[f"{location_type}_state_code"] is None or record[f"{location_type}_county_code"] is None:
            return None
        return {
            "country_code": record[f"{location_type}_country_code"],
            "state_code": record[f"{location_type}_state_code"],
            "state_fips": record[f"{location_type}_state_fips"],
            "county_code": record[f"{location_type}_county_code"],
            "county_name": record[f"{location_type}_county_name"],
            "population": record[f"{location_type}_county_population"],
        }

    def _congressional_agg_key(location_type):
        if record[f"{location_type}_state_code"] is None or record[f"{location_type}_congressional_code"] is None:
            return None
        return {
            "country_code": record[f"{location_type}_country_code"],
            "state_code": record[f"{location_type}_state_code"],
            "state_fips": record[f"{location_type}_state_fips"],
            "congressional_code": record[f"{location_type}_congressional_code"],
            "population": record[f"{location_type}_congressional_population"],
        }

    def _state_agg_key(location_type):
        if record[f"{location_type}_state_code"] is None:
            return None
        return {
            "country_code": record[f"{location_type}_country_code"],
            "state_code": record[f"{location_type}_state_code"],
            "state_name": record[f"{location_type}_state_name"],
            "population": record[f"{location_type}_state_population"],
        }

    def _country_agg_key(location_type):
        if record[f"{location_type}_country_code"] is None:
            return None
        return {
            "country_code",
            record[f"{location_type}_country_code"],
            "country_name",
            record[f"{location_type}_country_name"],
        }

    agg_key_func_lookup = {
        "awarding_subtier_agency_agg_key": lambda: _agency_agg_key("awarding", "subtier"),
        "awarding_toptier_agency_agg_key": lambda: _agency_agg_key("awarding", "toptier"),
        "funding_subtier_agency_agg_key": lambda: _agency_agg_key("funding", "subtier"),
        "funding_toptier_agency_agg_key": lambda: _agency_agg_key("funding", "toptier"),
        "naics_agg_key": _naics_agg_key,
        "pop_congressional_agg_key": lambda: _congressional_agg_key("pop"),
        "pop_country_agg_key": lambda: _country_agg_key("pop"),
        "pop_county_agg_key": lambda: _county_agg_key("pop"),
        "pop_state_agg_key": lambda: _state_agg_key("pop"),
        "psc_agg_key": _psc_agg_key,
        "recipient_agg_key": _recipient_agg_key,
        "recipient_location_congressional_agg_key": lambda: _congressional_agg_key("recipient_location"),
        "recipient_location_county_agg_key": lambda: _county_agg_key("recipient_location"),
        "recipient_location_state_agg_key": lambda: _state_agg_key("recipient_location"),
    }

    agg_key_func = agg_key_func_lookup.get(key_name)
    if agg_key_func is None:
        logger.error(f"The agg_key '{key_name}' is not valid.")

    agg_key = agg_key_func()
    if agg_key is None:
        return None
    return json.dumps(agg_key)
