"""
Jira Ticket Number(s): DEV-5943

    Create a new field which can determine distinct awards in FABA

Expected CLI:

    $ python3 usaspending_api/database_scripts/job_archive/backfill_submission_window_id.py

Purpose:

    Generates distinct_award_key for all FinancialAccountsByAwards records.

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

SQL = """
with submissions as (
    SELECT
        sa.submission_id,
        dsws.id as submission_window_id
    FROM submission_attributes sa
    LEFT OUTER JOIN dabs_submission_window_schedule dsws
      ON (
        sa.reporting_fiscal_year = dsws.submission_fiscal_year
        AND sa.reporting_fiscal_period = dsws.submission_fiscal_month
        AND sa.quarter_format_flag = dsws.is_quarter
    )
)
update  submission_attributes
set     submission_window_id = submissions.submission_window_id
from submissions
where
    submission_attributes.submission_id = submissions.submission_id
    and submission_attributes.submission_window_id is distinct from submissions.submission_window_id
"""


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


with Timer() as overall_timer:
    with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            with Timer() as t:
                cursor.execute(SQL.replace("\n", " "))


print(f"Finished. {TOTAL_UPDATES:,} rows with overall run time: {overall_timer.elapsed_as_string}")
