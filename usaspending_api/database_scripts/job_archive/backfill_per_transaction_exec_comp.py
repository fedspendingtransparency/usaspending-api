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


class Timer:

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args, **kwargs):
        self.end = time.perf_counter()
        self.elapsed = self.end - self.start
        self.elapsed_as_string = self.pretty_print(self.elapsed)

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


def run_broker_select_query(transaction_sql, transaction_type):
    with broker_connection.cursor() as cursor:
        print("Running Broker {} select query...".format(transaction_type))
        with Timer() as t:
            cursor.execute(transaction_sql)
        row_count = cursor.rowcount
        print("{} rows selected in {}".format(row_count, t.elapsed_as_string))
        return cursor.fetchall()


def run_spending_update_query(transaction_sql, transaction_type, broker_data):
    with spending_connection.cursor() as cursor:
        print("Running USAspending {} update query...".format(transaction_type))
        update_query = build_spending_update_query(transaction_sql, broker_data)
        with Timer() as t:
            cursor.execute(
                update_query,
                [col for row in broker_data for col in row]
            )
        row_count = cursor.rowcount
        print("{} rows updated in {}".format(row_count, t.elapsed_as_string))


with Timer() as overall_timer:
    with psycopg2.connect(dsn=SPENDING_CONNECTION_STRING) as spending_connection, \
            psycopg2.connect(dsn=BROKER_CONNECTION_STRING) as broker_connection:
        spending_connection.autocommit = True
        broker_fabs_data = run_broker_select_query(BROKER_FABS_SELECT_SQL, "FABS")
        run_spending_update_query(SPENDING_FABS_UPDATE_SQL, "FABS", broker_fabs_data)
        broker_fpds_data = run_broker_select_query(BROKER_FPDS_SELECT_SQL, "FPDS")
        run_spending_update_query(SPENDING_FPDS_UPDATE_SQL, "FPDS", broker_fpds_data)

print("Finished. Overall run time: {}".format(overall_timer.elapsed_as_string))

