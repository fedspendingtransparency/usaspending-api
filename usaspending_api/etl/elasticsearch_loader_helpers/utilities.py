import json
import logging
from collections import OrderedDict

import psycopg2

from dataclasses import dataclass
from django.conf import settings
from pathlib import Path
from random import choice
from typing import Optional, List

from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string

logger = logging.getLogger("script")


@dataclass
class WorkerNode:
    """Contains details for a worker node to perform micro ETL step"""

    name: str
    index: str
    sql: str
    primary_key: str
    is_incremental: bool
    transform_func: callable = None
    # ids: List[int] = field(default_factory=list)


def chunks(l, n):
    """Yield successive n-sized chunks from l"""
    for i in range(0, len(l), n):
        yield l[i : i + n]


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


def execute_sql_statement(cmd, results=False, verbose=False):
    """ Simple function to execute SQL using a psycopg2 connection"""
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


def db_rows_to_dict(cursor):
    """ Return a dictionary of all row results from a database connection cursor """
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def filter_query(column, values, query_type="match_phrase"):
    queries = [{query_type: {column: str(i)}} for i in values]
    return {"query": {"bool": {"should": [queries]}}}


def format_log(msg, process=None, job=None):
    inner_str = f"[{process if process else 'main'}] {f'{job}' if job else ''}"
    return f"{inner_str:<32} | {msg}"


def gen_random_name():
    """Generates (over) 5000 unique names in random order. Adds integer to names if necessary"""
    data_file = json.loads(Path(settings.APP_DIR / "data" / "multiprocessing_worker_names.json").read_text())
    iterations = 1
    max_combinations = len(data_file["attributes"]) * len(data_file["subjects"])
    name_template = "{attribute} {subject}"
    previous_names = []

    while True:
        name = name_template.format(attribute=choice(data_file["attributes"]), subject=choice(data_file["subjects"]))
        if name not in previous_names:
            previous_names.append(name)
            yield name

        if len(previous_names) >= max_combinations:
            iterations += 1
            max_combinations *= iterations
            name_template = "{attribute} {subject} {iterations}"


def create_agg_key(key_name: str, record: dict):
    """ Returns the json.dumps() for an Ordered Dictionary representing the agg_key """

    def _recipient_agg_key():
        if record["recipient_hash"] is None or record["recipient_levels"] is None:
            return OrderedDict(
                [
                    ("name", record["recipient_name"]),
                    ("unique_id", record["recipient_unique_id"]),
                    ("hash", None),
                    ("levels", None),
                ]
            )
        return OrderedDict(
            [
                ("name", record["recipient_name"]),
                ("unique_id", record["recipient_unique_id"]),
                ("hash", str(record["recipient_hash"])),
                ("levels", record["recipient_levels"]),
            ]
        )

    def _agency_agg_key(agency_type, agency_tier):
        if record[f"{agency_type}_{agency_tier}_agency_name"] is None:
            return None
        item_list = [("name", record[f"{agency_type}_{agency_tier}_agency_name"])]
        if f"{agency_type}_{agency_tier}_agency_abbreviation" in record:
            item_list.append(("abbreviation", record[f"{agency_type}_{agency_tier}_agency_abbreviation"]))
        if f"{agency_type}_{agency_tier}_agency_code" in record:
            item_list.append(("code", record[f"{agency_type}_{agency_tier}_agency_code"]))
        item_list.append(
            ("id", record[f"{agency_type}_{agency_tier + '_' if agency_tier == 'toptier' else ''}agency_id"])
        )
        return OrderedDict(item_list)

    def _cfda_agg_key():
        if record["cfda_number"] is None:
            return None
        return OrderedDict(
            [
                ("code", record["cfda_number"]),
                ("description", record["cfda_title"]),
                ("id", record["cfda_id"]),
                ("url", record["cfda_url"] if record["cfda_url"] != "None;" else None),
            ]
        )

    def _naics_agg_key():
        if record["naics_code"] is None:
            return None
        return OrderedDict([("code", record["naics_code"]), ("description", record["naics_description"])])

    def _psc_agg_key():
        if record["product_or_service_code"] is None:
            return None
        return OrderedDict(
            [("code", record["product_or_service_code"]), ("description", record["product_or_service_description"])]
        )

    def _county_agg_key(location_type):
        if record[f"{location_type}_state_code"] is None or record[f"{location_type}_county_code"] is None:
            return None
        return OrderedDict(
            [
                ("country_code", record[f"{location_type}_country_code"]),
                ("state_code", record[f"{location_type}_state_code"]),
                ("state_fips", record[f"{location_type}_state_fips"]),
                ("county_code", record[f"{location_type}_county_code"]),
                ("county_name", record[f"{location_type}_county_name"]),
                ("population", record[f"{location_type}_county_population"]),
            ]
        )

    def _congressional_agg_key(location_type):
        if record[f"{location_type}_state_code"] is None or record[f"{location_type}_congressional_code"] is None:
            return None
        return OrderedDict(
            [
                ("country_code", record[f"{location_type}_country_code"]),
                ("state_code", record[f"{location_type}_state_code"]),
                ("state_fips", record[f"{location_type}_state_fips"]),
                ("congressional_code", record[f"{location_type}_congressional_code"]),
                ("population", record[f"{location_type}_congressional_population"]),
            ]
        )

    def _state_agg_key(location_type):
        if record[f"{location_type}_state_code"] is None:
            return None
        return OrderedDict(
            [
                ("country_code", record[f"{location_type}_country_code"]),
                ("state_code", record[f"{location_type}_state_code"]),
                ("state_name", record[f"{location_type}_state_name"]),
                ("population", record[f"{location_type}_state_population"]),
            ]
        )

    def _country_agg_key(location_type):
        if record[f"{location_type}_country_code"] is None:
            return None
        return OrderedDict(
            [
                ("country_code", record[f"{location_type}_country_code"]),
                ("country_name", record[f"{location_type}_country_name"]),
            ]
        )

    agg_key_func_lookup = {
        "awarding_subtier_agency_agg_key": lambda: _agency_agg_key("awarding", "subtier"),
        "awarding_toptier_agency_agg_key": lambda: _agency_agg_key("awarding", "toptier"),
        "cfda_agg_key": _cfda_agg_key,
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

    agg_key = agg_key_func_lookup[key_name]()
    if agg_key is None:
        return None
    return json.dumps(agg_key)
