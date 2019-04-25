import math
import psycopg2
import time

from multiprocessing import Pool
from os import environ
from threading import Lock


# DEFINE THIS ENVIRONMENT VARIABLE BEFORE RUNNING!
# Obvs, this is the connection string to the database.
CONNECTION_STRING = environ['DATABASE_URL']


MULTIPROCESSING_POOLS = 4
CHUNK_SIZE = 50000


UPDATE_SQL = """
update  transaction_fabs
set     unique_award_key = upper(
            'ASST_AGG' || '_' ||
            coalesce(uri, '-NONE-') || '_' ||
            coalesce(awarding_sub_tier_agency_c, '-NONE-')
        )
where   record_type = 1 and transaction_id between {minid} and {maxid};


update  transaction_fabs
set     unique_award_key = upper(
            'ASST_NON' || '_' ||
            coalesce(fain, '-NONE-') || '_' ||
            coalesce(awarding_sub_tier_agency_c, '-NONE-')
        )
where   record_type is distinct from 1 and transaction_id between {minid} and {maxid};


update  transaction_fpds
set     unique_award_key = upper(
            'CONT_AWD' || '_' ||
            coalesce(piid, '-NONE-') || '_' ||
            coalesce(agency_id, '-NONE-') || '_' ||
            coalesce(parent_award_id, '-NONE-') || '_' ||
            coalesce(referenced_idv_agency_iden, '-NONE-')
        )
where   pulled_from = 'AWARD' and transaction_id between {minid} and {maxid};


update  transaction_fpds
set     unique_award_key = upper(
            'CONT_IDV' || '_' ||
            coalesce(piid, '-NONE-') || '_' ||
            coalesce(agency_id, '-NONE-')
        )
where   pulled_from is distinct from 'AWARD' and transaction_id between {minid} and {maxid};


update  transaction_normalized
set     unique_award_key = t.unique_award_key
from    transaction_fabs t
where   t.transaction_id = transaction_normalized.id and t.transaction_id between {minid} and {maxid};


update  transaction_normalized
set     unique_award_key = t.unique_award_key
from    transaction_fpds t
where   t.transaction_id = transaction_normalized.id and t.transaction_id between {minid} and {maxid};
"""


GET_MIN_MAX_SQL = 'select min(id), max(id) from transaction_normalized'


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
        return '%d:%02d:%02d.%04d' % (h, m, s, f*10000)


def execute_chunk(max_id, _min, _max, timer):
    try:
        with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                with Timer() as t:
                    cursor.execute(UPDATE_SQL.format(minid=_min, maxid=_max))
                row_count = cursor.rowcount
                print(
                    '[{:.2%}] {:,} => {:,}: {:,} updated in {} with an estimated remaining run time of {}'.format(
                        _max / max_id, _min, _max, row_count, t.elapsed_as_string,
                        timer.estimated_remaining_runtime(_max / max_id)
                    ),
                    flush=True
                )
    except Exception as e:
        print('Exception {:,} => {:,}: {}'.format(_min, _max, e))
        raise


failure_count = Counter()

with Timer() as overall_timer:

    with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
        with connection.cursor() as cursor:
            print('Finding min/max IDs...')
            cursor.execute(GET_MIN_MAX_SQL)
            results = cursor.fetchall()
            min_id, max_id = results[0]

    print('Min ID: {:,}'.format(min_id))
    print('Max ID: {:,}'.format(max_id), flush=True)

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

print('Finished.  Overall run time: %s' % overall_timer.elapsed_as_string)

if failure_count.value > 0:
    print('{:,} queries failed'.format(failure_count.value))
    exit(1)
