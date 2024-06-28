"""
Jira Ticket Number(s): DEV-7953

    Assist with changing the field type from CharField to TextField with minimal downtime.

Expected CLI:

    $ python3 usaspending_api/database_scripts/job_archive/backfill_faba_defc_as_text_field.py

Purpose:

    Copy the contents of "disaster_emergency_fund_code" into "disaster_emergency_fund_code_temp"
    to assist with changing the field type from CharField to TextField with minimal downtime.

    SINGLE PROCESS VERSION
"""

import math
import psycopg2
import time

from os import environ


# DEFINE THIS ENVIRONMENT VARIABLE BEFORE RUNNING!
# Obvs, this is the connection string to the database.
CONNECTION_STRING = environ["DATABASE_URL"]


CHUNK_SIZE = 50000
TOTAL_UPDATES = 0

SQLS = [
    """
UPDATE  financial_accounts_by_awards
SET     disaster_emergency_fund_code_temp = disaster_emergency_fund_code
WHERE   financial_accounts_by_awards_id BETWEEN {minid} AND {maxid}
""",
]


GET_MIN_MAX_SQL = "SELECT MIN(financial_accounts_by_awards_id), MAX(financial_accounts_by_awards_id) FROM financial_accounts_by_awards"


class Timer:
    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args, **kwargs):
        self.end = time.perf_counter()
        self.elapsed = self.end - self.start
        self.elapsed_as_string = self.pretty_print(self.elapsed)

    def snapshot(self):
        end = time.perf_counter()
        return self.pretty_print(end - self.start)

    def estimated_remaining_runtime(self, ratio):
        end = time.perf_counter()
        elapsed = end - self.start
        est = max((elapsed / ratio) - elapsed, 0.0)
        return self.pretty_print(est)

    @staticmethod
    def pretty_print(elapsed):
        f, s = math.modf(elapsed)
        m, s = divmod(s, 60)
        h, m = divmod(m, 60)
        return "%d:%02d:%02d.%04d" % (h, m, s, f * 10000)


def run_update_query():
    global TOTAL_UPDATES
    with connection.cursor() as cursor:
        with Timer() as t:
            cursor.execute(sql.format(minid=_min, maxid=_max))
        row_count = cursor.rowcount
        progress = (_max - min_id + 1 + totes * n) / (totes * len(SQLS))
        print(
            "[{:.2%}] {:,} => {:,}: {:,} updated in {} with an estimated remaining run time of {}".format(
                progress, _min, _max, row_count, t.elapsed_as_string, chunk_timer.estimated_remaining_runtime(progress)
            ),
            flush=True,
        )
        TOTAL_UPDATES += row_count


with Timer() as overall_timer:

    with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
        connection.autocommit = True

        with connection.cursor() as cursor:
            print("Finding min/max IDs...")
            cursor.execute(GET_MIN_MAX_SQL)
            results = cursor.fetchall()
            min_id, max_id = results[0]
            totes = max_id - min_id + 1

        print(f"Min ID: {min_id:,}")
        print(f"Max ID: {max_id:,}", flush=True)

        with Timer() as chunk_timer:
            for n, sql in enumerate(SQLS):
                _min = min_id
                while _min <= max_id:
                    _max = min(_min + CHUNK_SIZE - 1, max_id)
                    run_update_query()
                    _min = _max + 1

print(f"Finished. {TOTAL_UPDATES:,} rows with overall run time: {overall_timer.elapsed_as_string}")
