"""
Jira Ticket Number(s): DEV-2923, DEV-3030 (subtask)

    As a user/developer/tester I want the FABS and FPDS executive compensation data to match Broker.

Expected CLI:

    $ python usaspending_api/database_scripts/job_archive/backfill_per_transaction_exec_comp.py

Purpose:

    Fetch executive compensation data from Broker tables:
        - published_award_financial_assistance
        - detached_award_procurement
    And populate both FABS and FPDS transaction tables
"""
import math
import psycopg2
import time

from os import environ


# DEFINE THESE ENVIRONMENT VARIABLES BEFORE RUNNING!
SPENDING_CONNECTION_STRING = environ['DATABASE_URL']
BROKER_CONNECTION_STRING = environ['DATA_BROKER_DATABASE_URL']


BROKER_FABS_SELECT_SQL = """
select
    published_award_financial_assistance_id,
    high_comp_officer1_full_na,
    high_comp_officer1_amount,
    high_comp_officer2_full_na,
    high_comp_officer2_amount,
    high_comp_officer3_full_na,
    high_comp_officer3_amount,
    high_comp_officer4_full_na,
    high_comp_officer4_amount,
    high_comp_officer5_full_na,
    high_comp_officer5_amount
from
    published_award_financial_assistance
where
    published_award_financial_assistance_id between %(min_id)s and %(max_id)s and
    (
        high_comp_officer1_amount is not null or
        high_comp_officer1_full_na is not null or
        high_comp_officer2_amount is not null or
        high_comp_officer2_full_na is not null or
        high_comp_officer3_amount is not null or
        high_comp_officer3_full_na is not null or
        high_comp_officer4_amount is not null or
        high_comp_officer4_full_na is not null or
        high_comp_officer5_amount is not null or
        high_comp_officer5_full_na is not null
    )
"""

BROKER_FPDS_SELECT_SQL = """
select
    detached_award_procurement_id,
    high_comp_officer1_full_na,
    high_comp_officer1_amount,
    high_comp_officer2_full_na,
    high_comp_officer2_amount,
    high_comp_officer3_full_na,
    high_comp_officer3_amount,
    high_comp_officer4_full_na,
    high_comp_officer4_amount,
    high_comp_officer5_full_na,
    high_comp_officer5_amount
from
    detached_award_procurement
where
    detached_award_procurement_id between %(min_id)s and %(max_id)s and
    (
        high_comp_officer1_amount is not null or
        high_comp_officer1_full_na is not null or
        high_comp_officer2_amount is not null or
        high_comp_officer2_full_na is not null or
        high_comp_officer3_amount is not null or
        high_comp_officer3_full_na is not null or
        high_comp_officer4_amount is not null or
        high_comp_officer4_full_na is not null or
        high_comp_officer5_amount is not null or
        high_comp_officer5_full_na is not null
    )
"""

SPENDING_FABS_UPDATE_SQL = """
update
    transaction_fabs as fabs set
        officer_1_name = broker.high_comp_officer1_full_na,
        officer_1_amount = cast(broker.high_comp_officer1_amount as double precision),
        officer_2_name = broker.high_comp_officer2_full_na,
        officer_2_amount = cast(broker.high_comp_officer2_amount as double precision),
        officer_3_name = broker.high_comp_officer3_full_na,
        officer_3_amount = cast(broker.high_comp_officer3_amount as double precision),
        officer_4_name = broker.high_comp_officer4_full_na,
        officer_4_amount = cast(broker.high_comp_officer4_amount as double precision),
        officer_5_name = broker.high_comp_officer5_full_na,
        officer_5_amount = cast(broker.high_comp_officer5_amount as double precision)
from
    (values {}) as broker(
        published_award_financial_assistance_id,
        high_comp_officer1_full_na,
        high_comp_officer1_amount,
        high_comp_officer2_full_na,
        high_comp_officer2_amount,
        high_comp_officer3_full_na,
        high_comp_officer3_amount,
        high_comp_officer4_full_na,
        high_comp_officer4_amount,
        high_comp_officer5_full_na,
        high_comp_officer5_amount
    )
where
    fabs.published_award_financial_assistance_id = broker.published_award_financial_assistance_id
"""

SPENDING_FPDS_UPDATE_SQL = """
update
    transaction_fpds as fpds set
        officer_1_name = broker.high_comp_officer1_full_na,
        officer_1_amount = cast(broker.high_comp_officer1_amount as double precision),
        officer_2_name = broker.high_comp_officer2_full_na,
        officer_2_amount = cast(broker.high_comp_officer2_amount as double precision),
        officer_3_name = broker.high_comp_officer3_full_na,
        officer_3_amount = cast(broker.high_comp_officer3_amount as double precision),
        officer_4_name = broker.high_comp_officer4_full_na,
        officer_4_amount = cast(broker.high_comp_officer4_amount as double precision),
        officer_5_name = broker.high_comp_officer5_full_na,
        officer_5_amount = cast(broker.high_comp_officer5_amount as double precision)
from
    (values {}) as broker(
        detached_award_procurement_id,
        high_comp_officer1_full_na,
        high_comp_officer1_amount,
        high_comp_officer2_full_na,
        high_comp_officer2_amount,
        high_comp_officer3_full_na,
        high_comp_officer3_amount,
        high_comp_officer4_full_na,
        high_comp_officer4_amount,
        high_comp_officer5_full_na,
        high_comp_officer5_amount
    )
where
    fpds.detached_award_procurement_id = broker.detached_award_procurement_id
"""

GET_MIN_MAX_FABS_SQL = """
select
    min(published_award_financial_assistance_id), max(published_award_financial_assistance_id)
from
    published_award_financial_assistance
"""

GET_MIN_MAX_FPDS_SQL = """
select
    min(detached_award_procurement_id), max(detached_award_procurement_id)
from
    detached_award_procurement
"""

CHUNK_SIZE = 50000


class Timer:

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args, **kwargs):
        self.end = time.perf_counter()
        self.elapsed = self.end - self.start
        self.elapsed_as_string = self.pretty_print(self.elapsed)

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
        return "%d:%02d:%02d.%04d" % (h, m, s, f*10000)


def build_spending_update_query(query_base, update_data):
    values_string = ""
    for count, row in enumerate(update_data, 1):
        values_string += "("
        values_string += ",".join(["%s"] * len(row))
        values_string += ")"
        if count != len(update_data):
            values_string += ","
    return query_base.format(values_string)


def determine_progress():
    return (_max - min_id + 1) / total


def print_no_rows_to_update(transaction_type):
    progress = determine_progress()
    print(
        "[{} - {:.2%}] {:,} => {:,}: No rows to update with an estimated remaining run time of {}"
        .format(
            transaction_type, progress, _min, _max, chunk_timer.estimated_remaining_runtime(progress)
        ),
        flush=True
    )


def run_broker_select_query(transaction_sql):
    with broker_connection.cursor() as select_cursor:
        query_parameters = {
            "min_id": _min,
            "max_id": _max
        }
        select_cursor.execute(transaction_sql, query_parameters)
        return select_cursor.fetchall()


def run_spending_update_query(transaction_sql, transaction_type, broker_data):
    with spending_connection.cursor() as update_cursor:
        update_query = build_spending_update_query(transaction_sql, broker_data)
        with Timer() as t:
            update_cursor.execute(
                update_query,
                [col for row in broker_data for col in row]
            )
        row_count = update_cursor.rowcount
        progress = determine_progress()
        print(
            "[{} - {:.2%}] {:,} => {:,}: {:,} rows updated in {} with an estimated remaining run time of {}"
            .format(
                transaction_type, progress, _min, _max, row_count, t.elapsed_as_string,
                chunk_timer.estimated_remaining_runtime(progress)
            ),
            flush=True
        )
        return row_count


with Timer() as overall_timer:
    with psycopg2.connect(dsn=SPENDING_CONNECTION_STRING) as spending_connection, \
            psycopg2.connect(dsn=BROKER_CONNECTION_STRING) as broker_connection:
        spending_connection.autocommit = True

        print("Running FABS backfill from Broker to USAspending")

        fabs_row_count = 0

        with broker_connection.cursor() as cursor:
            print("Finding min/max Published_Award_Financial_Assistance_ID for FABS...")
            cursor.execute(GET_MIN_MAX_FABS_SQL)
            results = cursor.fetchall()
            min_id, max_id = results[0]
            total = max_id - min_id + 1

        print("Min Published_Award_Financial_Assistance_ID: {:,}".format(min_id))
        print("Max Published_Award_Financial_Assistance_ID: {:,}".format(max_id), flush=True)

        with Timer() as chunk_timer:
            _min = min_id
            while _min <= max_id:
                _max = min(_min + CHUNK_SIZE - 1, max_id)
                broker_fabs_data = run_broker_select_query(BROKER_FABS_SELECT_SQL)
                if broker_fabs_data:
                    updated_row_count = run_spending_update_query(SPENDING_FABS_UPDATE_SQL, "FABS", broker_fabs_data)
                    fabs_row_count += updated_row_count
                else:
                    print_no_rows_to_update("FABS")
                _min = _max + 1

        print(
            "Finished running FABS backfill. Took {} to update {:,} rows"
            .format(chunk_timer.elapsed_as_string, fabs_row_count)
        )

        print("Running FPDS backfill from Broker to USAspending")

        fpds_row_count = 0

        with broker_connection.cursor() as cursor:
            print("Finding min/max Detached_Award_Procurement_ID for FPDS...")
            cursor.execute(GET_MIN_MAX_FPDS_SQL)
            results = cursor.fetchall()
            min_id, max_id = results[0]
            total = max_id - min_id + 1

        print("Min Detached_Award_Procurement_ID: {:,}".format(min_id))
        print("Max Detached_Award_Procurement_ID: {:,}".format(max_id), flush=True)

        with Timer() as chunk_timer:
            _min = min_id
            while _min <= max_id:
                _max = min(_min + CHUNK_SIZE - 1, max_id)
                broker_fpds_data = run_broker_select_query(BROKER_FPDS_SELECT_SQL)
                if broker_fpds_data:
                    updated_row_count = run_spending_update_query(SPENDING_FPDS_UPDATE_SQL, "FPDS", broker_fpds_data)
                    fpds_row_count += updated_row_count
                else:
                    print_no_rows_to_update("FPDS")
                _min = _max + 1

        print(
            "Finished running FPDS backfill. Took {} to update {:,} rows"
            .format(chunk_timer.elapsed_as_string, fpds_row_count)
        )

print(
    "Finished. Overall run time to update {:,} rows: {}"
    .format(fabs_row_count + fpds_row_count, overall_timer.elapsed_as_string)
)
