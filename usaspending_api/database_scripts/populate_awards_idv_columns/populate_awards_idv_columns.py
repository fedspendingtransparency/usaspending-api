# There should be a readme.md file bundled with this script.  Please refer to it if you have any questions.
import math
import psycopg2
import time

from multiprocessing import Pool
from os import environ
from threading import Lock


# DEFINE THIS ENVIRONMENT VARIABLE BEFORE RUNNING!
# Obvs, this is the connection string to the database.
CONNECTION_STRING = environ["DATABASE_URL"]


MULTIPROCESSING_POOLS = 4  # This was chosen after much fine tuning.  Adding more might slow things down.
CHUNK_SIZE = 50000
TEMPORARY_TABLE_NAME = "temp_awards_ops494"

# Unlogged prevents updates to the log file.  This should be safe even in
# replication environments because we don't want this table replicated, only
# the final updates to the Awards tables.  But testing is your friend...
CREATE_TABLE = (
    """
    create unlogged table "%s" (
        id bigint,
        fpds_agency_id text,
        fpds_parent_agency_id text,
        base_exercised_options_val numeric(23, 2)
    )
"""
    % TEMPORARY_TABLE_NAME
)

POPULATE_TABLE = (
    """
    insert into
        "%s"
    select
        tx.award_id,
        max(case when f.transaction_id = a.latest_transaction_id then f.agency_id end)
            agency_id,
        max(case when f.transaction_id = a.latest_transaction_id then f.referenced_idv_agency_iden end)
            referenced_idv_agency_iden,
        sum(cast(f.base_exercised_options_val as numeric))
            base_exercised_options_val
    from
        vw_awards a
        inner join vw_transaction_normalized as tx on tx.award_id = a.id
        inner join vw_transaction_fpds as f on f.transaction_id = tx.id
    group by
        tx.award_id
"""
    % TEMPORARY_TABLE_NAME
)

ADD_PRIMARY_KEY = 'alter table "%s" add primary key (id)' % TEMPORARY_TABLE_NAME

GET_MIN_MAX_SQL = "select min(id), max(id) from vw_awards"

UPDATE_SQL = (
    """
    update
        award_search
    set
        fpds_agency_id = t.fpds_agency_id,
        fpds_parent_agency_id = t.fpds_parent_agency_id,
        base_exercised_options_val = t.base_exercised_options_val
    from
        "%s" t
    where
        t.id = awards.id and awards.id between {minid} and {maxid}
"""
    % TEMPORARY_TABLE_NAME
)

DROP_TABLE = 'drop table if exists "%s"' % TEMPORARY_TABLE_NAME

TABLE_EXISTS = (
    """
    select
        table_name
    from
        information_schema.tables
    where
        table_name = '%s' and table_schema = 'public'
"""
    % TEMPORARY_TABLE_NAME
)


class Counter(object):
    def __init__(self):
        self.value = 0
        self.lock = Lock()

    def increment(self, arg):
        with self.lock:
            self.value += 1


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

    def estimated_total_runtime(self, ratio):
        end = time.perf_counter()
        elapsed = end - self.start
        est = elapsed / ratio
        return self.pretty_print(est)

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


def execute_chunk(max_id, _min, _max, timer):
    try:
        with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
            connection.autocommit = True  # We are not concerned with transactions.
            with connection.cursor() as cursor:
                with Timer() as t:
                    cursor.execute(UPDATE_SQL.format(minid=_min, maxid=_max))
                row_count = cursor.rowcount
                print(
                    "[{:.2%}] {:,} => {:,}: {:,} updated in {} with an estimated remaining run time of {}".format(
                        _max / max_id,
                        _min,
                        _max,
                        row_count,
                        t.elapsed_as_string,
                        timer.estimated_remaining_runtime(_max / max_id),
                    ),
                    flush=True,
                )
    except Exception as e:
        print("Exception {:,} => {:,}: {}".format(_min, _max, e))
        raise


failure_count = Counter()

with Timer() as overall_timer:

    with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
        connection.autocommit = True  # We are not concerned with transactions.

        with connection.cursor() as cursor:
            print("Checking for temp table...")
            cursor.execute(TABLE_EXISTS)
            if cursor.rowcount > 0:
                print("Temp table found.  Not creating.")
            else:
                print("Temp table not found.  Creating.")
                cursor.execute(CREATE_TABLE)

                print("Populating temp table...", flush=True)
                with Timer() as t2:
                    cursor.execute(POPULATE_TABLE)
                print("Populated temp table in %s" % t2.elapsed_as_string)

                print("Adding primary key to temp table.", flush=True)
                with Timer() as t2:
                    cursor.execute(ADD_PRIMARY_KEY)
                print("Created primary key in %s" % t2.elapsed_as_string)

            print("Finding min/max IDs...")
            cursor.execute(GET_MIN_MAX_SQL)
            results = cursor.fetchall()
            min_id, max_id = results[0]

            print("Min ID: {:,}".format(min_id))
            print("Max ID: {:,}".format(max_id), flush=True)

        pool = Pool(MULTIPROCESSING_POOLS)

        with Timer() as chunk_timer:
            _min = min_id
            while _min <= max_id:
                _max = min(_min + CHUNK_SIZE - 1, max_id)
                pool.apply_async(
                    execute_chunk, (max_id, _min, _max, chunk_timer), error_callback=failure_count.increment
                )
                _min = _max + 1

        pool.close()
        pool.join()

        with connection.cursor() as cursor:
            print("Dropping temp table...")
            cursor.execute(DROP_TABLE)

print("Finished.  Overall run time: %s" % overall_timer.elapsed_as_string)

if failure_count.value > 0:
    print("{:,} queries failed".format(failure_count.value))
    exit(1)
