"""
Jira Ticket Number(s): DEV-8828

    Created new fields on Transaction Normalized and Awards for both the value of each transaction's
    "indirect_federal_sharing" amount and the sum of all amounts under an award, respectively.

Expected CLI:

    $ python3 usaspending_api/database_scripts/job_archive/backfill_fabs_indirect_federal_sharing.py

Purpose:

    Populate the FABS "indirect_federal_sharing_value" onto the Transaction Normalized record and add the sum
    to the Award record.

    SINGLE PROCESS VERSION
"""

import math
import psycopg2
import time

from os import environ


# DEFINE THIS ENVIRONMENT VARIABLE BEFORE RUNNING!
CONNECTION_STRING = environ["DATABASE_URL"]


CHUNK_SIZE = 200000
TOTAL_UPDATES = 0

SQL_LOOKUP = {
    "Transaction": {
        "min_max_sql": """
            SELECT MIN(transaction_id), MAX(transaction_id)
            FROM vw_transaction_fabs
            WHERE indirect_federal_sharing IS NOT NULL
        """,
        "update_sql": """
            UPDATE vw_transaction_normalized AS tn
            SET indirect_federal_sharing = fabs.indirect_federal_sharing
            FROM vw_transaction_fabs AS fabs
            WHERE fabs.transaction_id BETWEEN {min_id} AND {max_id}
                AND tn.id = fabs.transaction_id
                AND fabs.indirect_federal_sharing IS NOT NULL
        """,
    },
    "Award": {
        "min_max_sql": """
            SELECT MIN(award_id), MAX(award_id)
            FROM vw_transaction_normalized
            WHERE is_fpds = FALSE
                AND indirect_federal_sharing IS NOT NULL
        """,
        "update_sql": """
            WITH award_transaction_sum AS (
                SELECT tn.award_id, SUM(tn.indirect_federal_sharing) AS total_indirect_federal_sharing
                FROM vw_transaction_normalized AS tn
                WHERE tn.award_id BETWEEN {min_id} AND {max_id}
                    AND tn.is_fpds = FALSE
                    AND tn.indirect_federal_sharing IS NOT NULL
                GROUP BY tn.award_id
            )
            UPDATE award_search AS aw
            SET total_indirect_federal_sharing = award_transaction_sum.total_indirect_federal_sharing
            FROM award_transaction_sum
            WHERE aw.award_id = award_transaction_sum.award_id
        """,
    },
}


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


def run_update_query(backfill_type):
    global TOTAL_UPDATES
    sql = SQL_LOOKUP[backfill_type]["update_sql"]
    with connection.cursor() as cursor:
        with Timer() as t:
            cursor.execute(sql.format(min_id=_min, max_id=_max))
        row_count = cursor.rowcount
        progress = (_max - min_id + 1) / totals
        print(
            "[{}] [{:.2%}] {:,} => {:,}: {:,} updated in {} with an estimated remaining run time of {}".format(
                backfill_type,
                progress,
                _min,
                _max,
                row_count,
                t.elapsed_as_string,
                chunk_timer.estimated_remaining_runtime(progress),
            ),
            flush=True,
        )
        TOTAL_UPDATES += row_count


with Timer() as overall_timer:

    with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
        connection.autocommit = True

        for backfill_type, lookup in SQL_LOOKUP.items():
            min_max_sql = lookup["min_max_sql"]
            with connection.cursor() as cursor:
                print("Finding min/max IDs...")
                cursor.execute(min_max_sql)
                results = cursor.fetchall()
                min_id, max_id = results[0]
                totals = max_id - min_id + 1

            print(f"{backfill_type} Min ID: {min_id:,}")
            print(f"{backfill_type} Max ID: {max_id:,}", flush=True)

            with Timer() as chunk_timer:
                _min = min_id
                while _min <= max_id:
                    _max = min(_min + CHUNK_SIZE - 1, max_id)
                    run_update_query(backfill_type)
                    _min = _max + 1

print(f"Finished. {TOTAL_UPDATES:,} rows with overall run time: {overall_timer.elapsed_as_string}")
