import psycopg
from psycopg.rows import dict_row

from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string

SUBTIER_AGENCY_LIST_CACHE = {}


def _fetch_reference_data() -> None:
    global SUBTIER_AGENCY_LIST_CACHE
    with psycopg.connect(get_database_dsn_string()) as connection:
        with connection.cursor(row_factory=dict_row) as cursor:
            sql = (
                "SELECT * FROM subtier_agency "
                "JOIN agency "
                "ON subtier_agency.subtier_agency_id = agency.subtier_agency_id"
            )

            cursor.execute(sql)
            SUBTIER_AGENCY_LIST_CACHE = {result["subtier_code"]: result for result in cursor.fetchall()}


def subtier_agency_list() -> dict:
    """Returns all rows from subtier_agency table. Does NOT refresh if called twice,
    and does NOT make a copy that you can modify"""
    if not SUBTIER_AGENCY_LIST_CACHE:
        _fetch_reference_data()

    return SUBTIER_AGENCY_LIST_CACHE.copy()
