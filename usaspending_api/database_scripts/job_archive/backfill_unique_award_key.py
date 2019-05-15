"""
Jira Ticket Number(s): DEV-2273

    As a user/developer/tester I want a unified award key across Broker and USAspending so that when I am using
    the websites I can quickly find the data in the other system

Expected CLI:

    $ python usasepending_api/database_scripts/job_archive/backfill_unique_award_key.py

Purpose:

    Generates unique_award_keys for all transaction_fabs, transaction_fpds, and transaction_normalized records.

    SINGLE PROCESS VERSION
"""
import math
import psycopg2
import time

from os import environ


# DEFINE THIS ENVIRONMENT VARIABLE BEFORE RUNNING!
# Obvs, this is the connection string to the database.
CONNECTION_STRING = environ['DATABASE_URL']


CHUNK_SIZE = 50000


SQLS = ["""
update  transaction_fpds
set     unique_award_key = case
            when pulled_from = 'IDV' then
                upper(
                    'CONT_IDV' || '_' ||
                    coalesce(piid, '-NONE-') || '_' ||
                    coalesce(agency_id, '-NONE-')
                )
            else
                 upper(
                    'CONT_AWD' || '_' ||
                    coalesce(piid, '-NONE-') || '_' ||
                    coalesce(agency_id, '-NONE-') || '_' ||
                    coalesce(parent_award_id, '-NONE-') || '_' ||
                    coalesce(referenced_idv_agency_iden, '-NONE-')
                )
            end
where   transaction_id between {minid} and {maxid} and
        unique_award_key is null
""", """
update  transaction_fabs
set     unique_award_key = case
            when record_type = 1 then
                upper(
                    'ASST_AGG' || '_' ||
                    coalesce(uri, '-NONE-') || '_' ||
                    coalesce(awarding_sub_tier_agency_c, '-NONE-')
                )
            else
                upper(
                    'ASST_NON' || '_' ||
                    coalesce(fain, '-NONE-') || '_' ||
                    coalesce(awarding_sub_tier_agency_c, '-NONE-')
                )
        end
where   transaction_id between {minid} and {maxid} and
        unique_award_key is null
""", """
update  transaction_normalized
set     unique_award_key = t.unique_award_key
from    (
            select  tn.id, coalesce(fabs.unique_award_key, fpds.unique_award_key) unique_award_key
            from    transaction_normalized tn
                    left outer join transaction_fabs fabs on fabs.transaction_id = tn.id
                    left outer join transaction_fpds fpds on fpds.transaction_id = tn.id
            where   tn.id between {minid} and {maxid}
         ) t
where   transaction_normalized.id = t.id and
        transaction_normalized.unique_award_key is null
"""]


GET_MIN_MAX_SQL = 'select min(id), max(id) from transaction_normalized'


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
        return '%d:%02d:%02d.%04d' % (h, m, s, f*10000)


def run_update_query():
    with connection.cursor() as cursor:
        with Timer() as t:
            cursor.execute(sql.format(minid=_min, maxid=_max))
        row_count = cursor.rowcount
        progress = (_max + (max_id * n)) / (max_id * len(SQLS))
        print(
            '[{:.2%}] {:,} => {:,}: {:,} updated in {} with an estimated remaining run time of {}'
            .format(
                progress, _min, _max, row_count, t.elapsed_as_string,
                chunk_timer.estimated_remaining_runtime(progress)
            ),
            flush=True
        )


with Timer() as overall_timer:

    with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
        connection.autocommit = True

        with connection.cursor() as cursor:
            print('Finding min/max IDs...')
            cursor.execute(GET_MIN_MAX_SQL)
            results = cursor.fetchall()
            min_id, max_id = results[0]

        print('Min ID: {:,}'.format(min_id))
        print('Max ID: {:,}'.format(max_id), flush=True)

        with Timer() as chunk_timer:
            for n, sql in enumerate(SQLS):
                _min = min_id
                while _min <= max_id:
                    _max = min(_min + CHUNK_SIZE - 1, max_id)
                    run_update_query()
                    _min = _max + 1

print('Finished.  Overall run time: %s' % overall_timer.elapsed_as_string)
