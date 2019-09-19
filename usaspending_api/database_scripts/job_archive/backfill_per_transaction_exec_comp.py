"""
Jira Ticket Number(s): DEV-3331

    As a user/developer/tester I want the FABS and FPDS executive compensation data to match Broker.

Expected CLI:

    $ python3 usaspending_api/database_scripts/job_archive/backfill_per_transaction_exec_comp.py

Purpose:

    Fetch executive compensation data from Broker tables:
        - published_award_financial_assistance
        - detached_award_procurement
    And populate both FABS and FPDS transaction table columns:
        - officer_{1,5}_name
        - officer_{1,5}_amount
"""
import logging
import math
import psycopg2
import time

from os import environ


# DEFINE THESE ENVIRONMENT VARIABLES BEFORE RUNNING!
SPENDING_CONNECTION_STRING = environ["DATABASE_URL"]
BROKER_CONNECTION_STRING = environ["DATA_BROKER_DATABASE_URL"]


BROKER_FABS_SELECT_SQL = """
SELECT
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
FROM
    published_award_financial_assistance
WHERE
    published_award_financial_assistance_id BETWEEN %(min_id)s AND %(max_id)s AND
    (
        high_comp_officer1_amount IS NOT NULL OR
        high_comp_officer1_full_na IS NOT NULL OR
        high_comp_officer2_amount IS NOT NULL OR
        high_comp_officer2_full_na IS NOT NULL OR
        high_comp_officer3_amount IS NOT NULL OR
        high_comp_officer3_full_na IS NOT NULL OR
        high_comp_officer4_amount IS NOT NULL OR
        high_comp_officer4_full_na IS NOT NULL OR
        high_comp_officer5_amount IS NOT NULL OR
        high_comp_officer5_full_na IS NOT NULL
    )
"""

BROKER_FPDS_SELECT_SQL = """
SELECT
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
FROM
    detached_award_procurement
WHERE
    detached_award_procurement_id BETWEEN %(min_id)s AND %(max_id)s AND
    (
        high_comp_officer1_amount IS NOT NULL OR
        high_comp_officer1_full_na IS NOT NULL OR
        high_comp_officer2_amount IS NOT NULL OR
        high_comp_officer2_full_na IS NOT NULL OR
        high_comp_officer3_amount IS NOT NULL OR
        high_comp_officer3_full_na IS NOT NULL OR
        high_comp_officer4_amount IS NOT NULL OR
        high_comp_officer4_full_na IS NOT NULL OR
        high_comp_officer5_amount IS NOT NULL OR
        high_comp_officer5_full_na IS NOT NULL
    )
"""

SPENDING_FABS_UPDATE_SQL = """
UPDATE
    transaction_fabs AS fabs SET
        officer_1_name = broker.high_comp_officer1_full_na,
        officer_1_amount = broker.high_comp_officer1_amount::NUMERIC(23,2),
        officer_2_name = broker.high_comp_officer2_full_na,
        officer_2_amount = broker.high_comp_officer2_amount::NUMERIC(23,2),
        officer_3_name = broker.high_comp_officer3_full_na,
        officer_3_amount = broker.high_comp_officer3_amount::NUMERIC(23,2),
        officer_4_name = broker.high_comp_officer4_full_na,
        officer_4_amount = broker.high_comp_officer4_amount::NUMERIC(23,2),
        officer_5_name = broker.high_comp_officer5_full_na,
        officer_5_amount = broker.high_comp_officer5_amount::NUMERIC(23,2)
FROM
    (VALUES {}) AS broker(
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
WHERE
    fabs.published_award_financial_assistance_id = broker.published_award_financial_assistance_id
    AND (
        officer_1_name IS DISTINCT FROM broker.high_comp_officer1_full_na
        OR officer_1_amount IS DISTINCT FROM broker.high_comp_officer1_amount::NUMERIC(23,2)
        OR officer_2_name IS DISTINCT FROM broker.high_comp_officer2_full_na
        OR officer_2_amount IS DISTINCT FROM broker.high_comp_officer2_amount::NUMERIC(23,2)
        OR officer_3_name IS DISTINCT FROM broker.high_comp_officer3_full_na
        OR officer_3_amount IS DISTINCT FROM broker.high_comp_officer3_amount::NUMERIC(23,2)
        OR officer_4_name IS DISTINCT FROM broker.high_comp_officer4_full_na
        OR officer_4_amount IS DISTINCT FROM broker.high_comp_officer4_amount::NUMERIC(23,2)
        OR officer_5_name IS DISTINCT FROM broker.high_comp_officer5_full_na
        OR officer_5_amount IS DISTINCT FROM broker.high_comp_officer5_amount::NUMERIC(23,2)
    )
"""

SPENDING_FPDS_UPDATE_SQL = """
UPDATE
    transaction_fpds AS fpds SET
        officer_1_name = broker.high_comp_officer1_full_na,
        officer_1_amount = broker.high_comp_officer1_amount::NUMERIC(23,2),
        officer_2_name = broker.high_comp_officer2_full_na,
        officer_2_amount = broker.high_comp_officer2_amount::NUMERIC(23,2),
        officer_3_name = broker.high_comp_officer3_full_na,
        officer_3_amount = broker.high_comp_officer3_amount::NUMERIC(23,2),
        officer_4_name = broker.high_comp_officer4_full_na,
        officer_4_amount = broker.high_comp_officer4_amount::NUMERIC(23,2),
        officer_5_name = broker.high_comp_officer5_full_na,
        officer_5_amount = broker.high_comp_officer5_amount::NUMERIC(23,2)
FROM
    (VALUES {}) AS broker(
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
WHERE
    fpds.detached_award_procurement_id = broker.detached_award_procurement_id
    AND (
        officer_1_name IS DISTINCT FROM broker.high_comp_officer1_full_na
        OR officer_1_amount IS DISTINCT FROM broker.high_comp_officer1_amount::NUMERIC(23,2)
        OR officer_2_name IS DISTINCT FROM broker.high_comp_officer2_full_na
        OR officer_2_amount IS DISTINCT FROM broker.high_comp_officer2_amount::NUMERIC(23,2)
        OR officer_3_name IS DISTINCT FROM broker.high_comp_officer3_full_na
        OR officer_3_amount IS DISTINCT FROM broker.high_comp_officer3_amount::NUMERIC(23,2)
        OR officer_4_name IS DISTINCT FROM broker.high_comp_officer4_full_na
        OR officer_4_amount IS DISTINCT FROM broker.high_comp_officer4_amount::NUMERIC(23,2)
        OR officer_5_name IS DISTINCT FROM broker.high_comp_officer5_full_na
        OR officer_5_amount IS DISTINCT FROM broker.high_comp_officer5_amount::NUMERIC(23,2)
    )
"""

GET_MIN_MAX_FABS_SQL = """
SELECT
    min(published_award_financial_assistance_id), max(published_award_financial_assistance_id)
FROM
    published_award_financial_assistance
"""

GET_MIN_MAX_FPDS_SQL = """
SELECT
    min(detached_award_procurement_id), max(detached_award_procurement_id)
FROM
    detached_award_procurement
"""

CHUNK_SIZE = 1000000


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
        return "%d:%02d:%02d.%04d" % (h, m, s, f * 10000)


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
    logging.info(
        "[{} - {:.2%}] {:,} => {:,}: No rows to update with an estimated remaining run time of {}".format(
            transaction_type, progress, _min, _max, chunk_timer.estimated_remaining_runtime(progress)
        )
    )


def run_broker_select_query(transaction_sql):
    with broker_connection.cursor() as select_cursor:
        query_parameters = {"min_id": _min, "max_id": _max}
        select_cursor.execute(transaction_sql, query_parameters)
        return select_cursor.fetchall()


def run_spending_update_query(transaction_sql, transaction_type, broker_data):
    with spending_connection.cursor() as update_cursor:
        update_query = build_spending_update_query(transaction_sql, broker_data)
        with Timer() as t:
            update_cursor.execute(update_query, [col for row in broker_data for col in row])
        row_count = update_cursor.rowcount
        progress = determine_progress()
        logging.info(
            "[{} - {:.2%}] {:,} => {:,}: {:,} rows updated in {} with an estimated remaining run time of {}".format(
                transaction_type,
                progress,
                _min,
                _max,
                row_count,
                t.elapsed_as_string,
                chunk_timer.estimated_remaining_runtime(progress),
            ),
            flush=True,
        )
        return row_count


if __name__ == "__main__":
    log_format = "[%(asctime)s] [%(levelname)s] - %(message)s"
    logging.getLogger()
    logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y/%m/%d %H:%M:%S (%Z)")

    with Timer() as overall_timer:
        with psycopg2.connect(dsn=SPENDING_CONNECTION_STRING) as spending_connection, psycopg2.connect(
            dsn=BROKER_CONNECTION_STRING
        ) as broker_connection:
            spending_connection.autocommit = True

            logging.info("Running FABS backfill from Broker to USAspending")

            fabs_row_count = 0

            with broker_connection.cursor() as cursor:
                logging.info("Finding min/max Published_Award_Financial_Assistance_ID for FABS...")
                cursor.execute(GET_MIN_MAX_FABS_SQL)
                results = cursor.fetchall()
                min_id, max_id = results[0]
                total = max_id - min_id + 1

            logging.info("Min Published_Award_Financial_Assistance_ID: {:,}".format(min_id))
            logging.info("Max Published_Award_Financial_Assistance_ID: {:,}".format(max_id))

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
                    # Move to next chunk
                    _min = _max + 1

            logging.info(
                "Finished running FABS backfill. Took {} to update {:,} rows".format(
                    chunk_timer.elapsed_as_string, fabs_row_count
                )
            )

            logging.info("Running FPDS backfill from Broker to USAspending")

            fpds_row_count = 0

            with broker_connection.cursor() as cursor:
                logging.info("Finding min/max Detached_Award_Procurement_ID for FPDS...")
                cursor.execute(GET_MIN_MAX_FPDS_SQL)
                results = cursor.fetchall()
                min_id, max_id = results[0]
                total = max_id - min_id + 1

            logging.info("Min Detached_Award_Procurement_ID: {:,}".format(min_id))
            logging.info("Max Detached_Award_Procurement_ID: {:,}".format(max_id))

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
                    # Move to next chunk
                    _min = _max + 1

            logging.info(
                "Finished running FPDS backfill. Took {} to update {:,} rows".format(
                    chunk_timer.elapsed_as_string, fpds_row_count
                )
            )

    logging.info(
        "Finished. Overall run time to update {:,} rows: {}".format(
            fabs_row_count + fpds_row_count, overall_timer.elapsed_as_string
        )
    )
