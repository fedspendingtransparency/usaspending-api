"""
Jira Ticket Number: DEV-11876

    Change Broker PARK columns in USAspending

Expected CLI:

    $ python3 usaspending_api/database_scripts/job_archive/backfill_renamed_park_fields.py

Purpose:

    Multiple fields were renamed in Broker and thus renamed in USAspending accordingly.
    Performing a single RENAME operation will hold an AccessExclusiveLock on the DB and
    potentially interfere with normal operations of the API and Downloads. To prevent any
    outage this script iterates over all File B and C records to copy values from the old
    columns to their renamed counterparts, taking a lock on the File B and C tables intermittently.

"""

import logging

from os import environ
from pathlib import Path
from typing import Iterator, Tuple

import psycopg2

# Import our USAspending Timer component.  This will not work if we ever add
# any Django specific stuff to the timing_helpers.py file.
exec(Path("usaspending_api/common/helpers/timing_helpers.py").read_text())

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] [%(levelname)s] - %(message)s", datefmt="%Y-%m-%d %H:%M:%S %Z"
)


# Simplify instantiations of Timer to automatically use the correct logger.
class Timer(Timer):  # noqa - this is imported indirectly and will fail flake8 check
    def __init__(self, message=None):
        super().__init__(message=message, success_logger=logging.info, failure_logger=logging.error)


def id_ranges(min_id: int, max_id: int) -> Iterator[Tuple[int, int]]:
    chunk_size = 500_000
    for n in range(min_id, max_id + 1, chunk_size):
        yield n, min(n + chunk_size, max_id)


def get_min_max_ids(conn: psycopg2.extensions.connection, table_name: str, primary_key: str) -> Tuple[int, int]:
    with Timer(f'collecting MIN and MAX values for "{table_name}"."{primary_key}"') as t:
        with conn.cursor() as cursor:
            sql = f"""
                SELECT min({primary_key}), max({primary_key})
                FROM   {table_name}
                WHERE  pa_reporting_key IS NOT NULL
                       OR ussgl480110_reinstated_del_cpe IS NOT NULL
                       OR ussgl490110_reinstated_del_cpe IS NOT NULL
            """
            cursor.execute(sql)
            min_id, max_id = cursor.fetchone()

            # Using -1 to denote that no valid ID values were found
            min_id = min_id or -1
            max_id = max_id or -1

    logging.info(f'Found MIN and MAX values ({min_id:,d} to {max_id:,d}) for "{table_name}"."{primary_key}" in {t}')
    return min_id, max_id


def run_update(
    conn: psycopg2.extensions.connection, table_name: str, primary_key: str, min_id: int, max_id: int
) -> None:
    total_row_count = 0
    estimated_id_count = max_id - min_id + 1
    with Timer(f'updating PARK values for "{table_name}"."{primary_key}"') as t:
        for chunk_min_id, chunk_max_id in id_ranges(min_id, max_id):
            with conn.cursor() as cursor:
                sql = f"""
                    UPDATE {table_name}
                    SET    program_activity_reporting_key = pa_reporting_key,
                           ussgl480110_rein_undel_ord_cpe = ussgl480110_reinstated_del_cpe,
                           ussgl490110_rein_deliv_ord_cpe = ussgl490110_reinstated_del_cpe
                    WHERE  {primary_key} >= {chunk_min_id}
                           AND {primary_key} <= {chunk_max_id}
                """
                cursor.execute(sql)
            row_count = cursor.rowcount
            total_row_count += row_count
            ratio = (chunk_max_id - min_id + 1) / estimated_id_count
            logging.info(
                f'Updated {row_count:,d} rows with "{primary_key}" between {chunk_min_id:,d} and {chunk_max_id:,d}.'
                f" Estimated time remaining: {t.estimated_remaining_runtime(ratio)}"
            )

    logging.info(f'Finished updating {total_row_count:,d} rows for "{table_name}"."{primary_key}" in {t}')


if __name__ == "__main__":

    tables_to_update = ["financial_accounts_by_program_activity_object_class", "financial_accounts_by_awards"]

    # In psycopg2 v2.9 it was changed such that connections created as a context manager will utilize
    # a transaction regardless of the "autocommit" setting. To avoid transactions we instead instantiate
    # the connection and then make sure to close it.
    connection = psycopg2.connect(dsn=environ["DATABASE_URL"])
    connection.autocommit = True

    try:
        for temp_table_name in tables_to_update:
            temp_primary_key = f"{temp_table_name}_id"
            overall_min_id, overall_max_id = get_min_max_ids(connection, temp_table_name, temp_primary_key)
            run_update(connection, temp_table_name, temp_primary_key, overall_min_id, overall_max_id)
    finally:
        connection.close()
