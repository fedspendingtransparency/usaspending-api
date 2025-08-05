"""
Jira Ticket Number(s): DEV-2376

    As a user/developer/tester I want the FABS uniqueness key to match Broker and include CFDA numbers

Expected CLI:

    $ python usaspending_api/database_scripts/job_archive/backfill_fabs_unique_transaction_key.py

Purpose:

    Generates afa_generated_unique for all transaction_fabs and fabs transaction_normalized records.

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


SQLS = [
    """
update  transaction_fabs
set     afa_generated_unique = upper(
                                    coalesce(awarding_sub_tier_agency_c, '-NONE-') || '_' ||
                                    coalesce(fain, '-NONE-') || '_' ||
                                    coalesce(uri, '-NONE-') || '_' ||
                                    coalesce(cfda_number, '-NONE-') || '_' ||
                                    coalesce(award_modification_amendme, '-NONE-')
                                )
where   transaction_id between {minid} and {maxid}
""",
    """
update  transaction_normalized
set     transaction_unique_id = transaction_fabs.afa_generated_unique
from    transaction_fabs
where   transaction_normalized.id = transaction_fabs.transaction_id and
        transaction_fabs.transaction_id between {minid} and {maxid}
""",
]


GET_MIN_MAX_SQL = "select min(transaction_id), max(transaction_id) from transaction_fabs"


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
    with connection.cursor() as cursor:
        with Timer() as t:
            cursor.execute(sql.format(minid=_min, maxid=_max))
        row_count = cursor.rowcount
        progress = progress = (_max - min_id + 1 + totes * n) / (totes * len(SQLS))
        print(
            "[{:.2%}] {:,} => {:,}: {:,} updated in {} with an estimated remaining run time of {}".format(
                progress, _min, _max, row_count, t.elapsed_as_string, chunk_timer.estimated_remaining_runtime(progress)
            ),
            flush=True,
        )


with Timer() as overall_timer:

    with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
        connection.autocommit = True

        with connection.cursor() as cursor:
            print("Finding min/max IDs...")
            cursor.execute(GET_MIN_MAX_SQL)
            results = cursor.fetchall()
            min_id, max_id = results[0]
            totes = max_id - min_id + 1

        print("Min ID: {:,}".format(min_id))
        print("Max ID: {:,}".format(max_id), flush=True)

        with Timer() as chunk_timer:
            for n, sql in enumerate(SQLS):
                _min = min_id
                while _min <= max_id:
                    _max = min(_min + CHUNK_SIZE - 1, max_id)
                    run_update_query()
                    _min = _max + 1

print("Finished.  Overall run time: %s" % overall_timer.elapsed_as_string)
