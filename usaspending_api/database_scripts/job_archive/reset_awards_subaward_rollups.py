"""
Jira Ticket Number(s): DEV-2116

    DEV-2116 is about integrating Broker's new subaward table into USAspending.

Expected CLI:

    $ python usaspending_api/database_scripts/job_archive/reset_awards_subaward_rollups.py

Purpose:

    As part of DEV-2116, we will need to fully reload the subaward table.  It
    was noticed during testing that awards without a subaward have inconsistent
    "default" values - some are NULL and others are 0.  The default is supposed
    to be NULL for the dollar figure and 0 for the count, based on the model
    definition.

    This script will run through all of the awards and fix the values for awards
    that have no subawards.  This will prevent the actual subaward rebuild from
    taking an immense amount of time, potentially locking tables for hours on end.

    The single SQL script version took 5 hours-ish on DEV.  We're hoping this
    script will pare that number down a bit and, since we're chunking/batching,
    will not hold locks for extended periods of time.

    This script can be eliminated after sprint 85 has finished rolling out and
    we are happy with the results.

"""

import math
import psycopg2
import time

from datetime import timedelta
from os import environ


# DEFINE THIS ENVIRONMENT VARIABLE BEFORE RUNNING!
# Obvs, this is the connection string to the database.
CONNECTION_STRING = environ["DATABASE_URL"]


CHUNK_SIZE = 500000


CREATE_TEMP_SUBAWARD_AWARD_ID_TABLE_SQL = """
    drop table if exists    temp_dev_2006_subaward_award_ids;

    create unlogged table   temp_dev_2006_subaward_award_ids
    as select distinct      award_id
    from                    subaward
    where                   award_id is not null;
"""


INDEX_TEMP_TABLE_SQL = """
    create unique index     idx_temp_dev_2006_subaward_award_ids
    on                      temp_dev_2006_subaward_award_ids (award_id)
"""


GET_MIN_MAX_SQL = "select min(id), max(id) from vw_awards"


# Reset award subaward rollups to their defaults for awards that do not have
# subawards and where they are not already the correct value.
UPDATE_AWARDS_CHUNK_SQL = """
    update
        award_search

    set
        total_subaward_amount = null,
        subaward_count = 0

    where
        id between {minid} and {maxid} and
        id not in (
            select award_id from temp_dev_2006_subaward_award_ids where award_id between {minid} and {maxid}
        ) and
        (
            total_subaward_amount is not null or
            subaward_count is distinct from 0
        )
"""


DROP_TEMP_TABLE_SQL = "drop table if exists temp_dev_2006_subaward_award_ids"


class Timer:
    """
    Stolen from usaspending_api/common/helpers/timing_helpers.Timer which,
    unfortunately, we can't use since this is a standalone (throwaway) script.
    """

    _formats = "{:,} d", "{} h", "{} m", "{} s", "{} ms"

    def __init__(self, message=None, success_logger=print, failure_logger=print):
        self.message = message
        self.success_logger = success_logger
        self.failure_logger = failure_logger
        self.start()

    def __enter__(self):
        self.log_starting_message()
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        if exc_type is None:
            self.log_success_message()
        else:
            self.log_failure_message()

    def __repr__(self):
        return self.as_string(self.elapsed)

    def start(self):
        self._start = time.perf_counter()
        self._stop = None
        self._elapsed = None

    def stop(self):
        self._stop = time.perf_counter()
        self._elapsed = timedelta(seconds=(self._stop - self._start))

    def log_starting_message(self):
        if self.message:
            self.success_logger(self.starting_message)

    @property
    def starting_message(self):
        return "[{}] starting...".format(self.message)

    def log_success_message(self):
        if self.message:
            self.success_logger(self.success_message)

    @property
    def success_message(self):
        return "[{}] finished successfully after {}".format(self.message, self)

    def log_failure_message(self):
        if self.message:
            self.failure_logger(self.failure_message)

    @property
    def failure_message(self):
        return "[{}] FAILED AFTER {}".format(self.message, self)

    @property
    def elapsed(self):
        if self._start is None:
            raise RuntimeError("Timer has not been started")
        if self._elapsed is None:
            return timedelta(seconds=(time.perf_counter() - self._start))
        return self._elapsed

    def estimated_total_runtime(self, ratio):
        if self._start is None:
            raise RuntimeError("Timer has not been started")
        if self._elapsed is None:
            return timedelta(seconds=((time.perf_counter() - self._start) / ratio))
        return self._elapsed

    def estimated_remaining_runtime(self, ratio):
        if self._elapsed is None:
            return max(self.estimated_total_runtime(ratio) - self.elapsed, timedelta())
        return timedelta()  # 0

    @classmethod
    def as_string(cls, elapsed):
        """elapsed should be a timedelta"""
        f, s = math.modf(elapsed.total_seconds())
        ms = round(f * 1000)
        m, s = divmod(s, 60)
        h, m = divmod(m, 60)
        d, h = divmod(h, 24)

        return (
            " ".join(f.format(b) for f, b in zip(cls._formats, tuple(int(n) for n in (d, h, m, s, ms))) if b > 0)
            or "less than a millisecond"
        )


def execute_sql(sql):
    with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(sql)
            if cursor.rowcount > -1:
                return cursor.rowcount
            return 0


def log_execute_sql(sql):
    rowcount = execute_sql(sql)
    print("{:,} rows affected".format(rowcount))
    return rowcount


def get_min_max_ids():
    with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
        with connection.cursor() as cursor:
            cursor.execute(GET_MIN_MAX_SQL)
            return cursor.fetchall()[0]


def enumerate_id_chunks(min_id, max_id):
    _min = min_id
    while _min <= max_id:
        _max = min(_min + CHUNK_SIZE - 1, max_id)
        yield _min, _max
        _min = _max + 1


if __name__ == "__main__":

    with Timer(__file__):

        with Timer("create temp table"):
            log_execute_sql(CREATE_TEMP_SUBAWARD_AWARD_ID_TABLE_SQL)

        with Timer("index temp table"):
            execute_sql(INDEX_TEMP_TABLE_SQL)

        with Timer("get min/max ids"):
            min_id, max_id = get_min_max_ids()

        total_affected = 0

        with Timer("update awards") as uat:
            id_diff = max_id - min_id + 1
            for min_chunk_id, max_chunk_id in enumerate_id_chunks(min_id, max_id):
                sql = UPDATE_AWARDS_CHUNK_SQL.format(minid=min_chunk_id, maxid=max_chunk_id)
                with Timer() as t:
                    affected = execute_sql(sql)

                total_affected += affected
                ratio = (max_chunk_id - min_id + 1) / id_diff
                remaining = uat.estimated_remaining_runtime((max_chunk_id - min_id + 1) / id_diff)
                print(
                    "[{:.2%}] {:,} => {:,}: {:,} updated in {} with an estimated remaining run time of {}".format(
                        ratio, min_chunk_id, max_chunk_id, affected, t.as_string(t.elapsed), t.as_string(remaining)
                    )
                )

        print("{:,} awards updated in total".format(total_affected))

        with Timer("drop temp table"):
            execute_sql(DROP_TEMP_TABLE_SQL)
