import json
import logging
import psycopg2

from typing import Optional

from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string

logger = logging.getLogger("script")


class DataJob:
    def __init__(self, *args):
        self.name = args[0]
        self.index = args[1]
        self.fy = args[2]
        self.csv = args[3]
        self.count = None


def convert_postgres_array_as_string_to_list(array_as_string: str) -> Optional[list]:
    """
        Postgres arrays are stored in CSVs as strings. Elasticsearch is able to handle lists of items, but needs to
        be passed a list instead of a string. In the case of an empty array, return null.
        For example, "{this,is,a,postgres,array}" -> ["this", "is", "a", "postgres", "array"].
    """
    return array_as_string[1:-1].split(",") if len(array_as_string) > 2 else None


def convert_postgres_json_array_as_string_to_list(json_array_as_string: str) -> Optional[dict]:
    """
        Postgres JSON arrays (jsonb) are stored in CSVs as strings. Since we want to avoid nested types
        in Elasticsearch the JSON arrays are converted to dictionaries to make parsing easier and then
        converted back into a formatted string.
    """
    if json_array_as_string is None or len(json_array_as_string) == 0:
        return None
    result = []
    json_array = json.loads(json_array_as_string)
    for j in json_array:
        for key, value in j.items():
            j[key] = "" if value is None else str(j[key])
        result.append(json.dumps(j, sort_keys=True))
    return result


def process_guarddog(process_list):
    """
        pass in a list of multiprocess Process objects.
        If one errored then terminate the others and return True
    """
    for proc in process_list:
        # If exitcode is None, process is still running. exit code 0 is normal
        if proc.exitcode not in (None, 0):
            msg = f"Script proccess failed!!! {proc.name} exited with error {proc.exitcode}. Terminating all processes."
            logger.error(format_log(msg))
            [x.terminate() for x in process_list]
            return True
    return False


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


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


def format_log(msg, process=None, job=None):
    inner_str = f"[{process if process else 'main'}] {f'(#{job})' if job else ''}"
    return f"{inner_str:<18} | {msg}"
