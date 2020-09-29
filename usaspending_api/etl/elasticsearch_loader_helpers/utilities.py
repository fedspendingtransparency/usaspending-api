import json
import logging
import psycopg2

from typing import Optional, List
from dataclasses import dataclass
from random import choice

from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string

logger = logging.getLogger("script")


@dataclass
class WorkerNode:
    """Contains details for a worker node to perform micro ETL step"""

    name: str
    index: str
    sql: str
    load_type: str
    transform_func: callable
    # ids: List[int] = field(default_factory=list)


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
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
        for key, value in j.items():
            j[key] = "" if value is None else str(j[key])
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
    """Generates over 600 unique name strings, random order each run"""
    previous_names = []

    nouns = [
        "Agent",
        "Archer",
        "Armadillo",
        "Champion",
        "Crusher",
        "Dart",
        "Defender",
        "Dragon",
        "Enchanter",
        "Falcon",
        "Gargoyle",
        "Hammer",
        "Mantis",
        "Mastermind",
        "Omen",
        "Phoenix",
        "Puma",
        "Seer",
        "Shadow",
        "Slayer",
        "Spectacle",
        "Warrior",
        "Wizard",
        "Wonder",
    ]

    prefix = [
        "Black",
        "Blue",
        "Capped",
        "Colossal",
        "Commander",
        "Dark",
        "Doctor",
        "Eager",
        "Earth",
        "Ethereal",
        "Gentle",
        "Giant",
        "Green",
        "Grey",
        "Heavy",
        "Humble",
        "Kind",
        "Mighty",
        "Nefarious",
        "Professor",
        "Purple",
        "Red",
        "Sassy",
        "Speedy",
        "Thunder",
        "White",
        "Yellow",
    ]

    max_combinations = len(prefix) * len(nouns)

    while True:
        name = f"{choice(prefix)} {choice(nouns)}"
        if name not in previous_names:
            previous_names.append(name)
            yield name

        if len(previous_names) >= max_combinations:
            break
