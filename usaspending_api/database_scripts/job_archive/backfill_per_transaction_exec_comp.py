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

from datetime import datetime, timezone
from os import environ


# DEFINE THESE ENVIRONMENT VARIABLES BEFORE RUNNING!
SPENDING_CONNECTION_STRING = environ["DATABASE_URL"]
BROKER_CONNECTION_STRING = environ["DATA_BROKER_DATABASE_URL"]


BROKER_FABS_SELECT_SQL = """
SELECT
    published_award_financial_assistance_id,
    UPPER(high_comp_officer1_full_na),
    high_comp_officer1_amount,
    UPPER(high_comp_officer2_full_na),
    high_comp_officer2_amount,
    UPPER(high_comp_officer3_full_na),
    high_comp_officer3_amount,
    UPPER(high_comp_officer4_full_na),
    high_comp_officer4_amount,
    UPPER(high_comp_officer5_full_na),
    high_comp_officer5_amount
FROM
    published_award_financial_assistance
WHERE
    published_award_financial_assistance_id IN %s
"""

BROKER_FPDS_SELECT_SQL = """
SELECT
    detached_award_procurement_id,
    UPPER(high_comp_officer1_full_na),
    high_comp_officer1_amount,
    UPPER(high_comp_officer2_full_na),
    high_comp_officer2_amount,
    UPPER(high_comp_officer3_full_na),
    high_comp_officer3_amount,
    UPPER(high_comp_officer4_full_na),
    high_comp_officer4_amount,
    UPPER(high_comp_officer5_full_na),
    high_comp_officer5_amount
FROM
    detached_award_procurement
WHERE
    detached_award_procurement_id IN %s
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
RETURNING fabs.transaction_id
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
RETURNING fpds.transaction_id
"""

ADD_TRANSACTIONS_TO_DELTA = """
INSERT INTO transaction_delta (transaction_id, created_at)
VALUES {} ON CONFLICT (transaction_id) DO NOTHING
"""

GET_FABS_IDS_WITH_EC_SQL = """
SELECT published_award_financial_assistance_id
FROM published_award_financial_assistance
WHERE high_comp_officer1_full_na IS NOT NULL
"""

GET_FPDS_IDS_WITH_EC_SQL = """
SELECT detached_award_procurement_id
FROM detached_award_procurement
WHERE high_comp_officer1_full_na IS NOT NULL
"""

CHUNK_SIZE = 25000


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


def print_no_rows_to_update(transaction_type):
    logging.info("[{}] No rows to update".format(transaction_type))


def run_broker_select_query(transaction_sql, id_tuple):
    with broker_connection.cursor() as select_cursor:
        select_cursor.execute(transaction_sql, [id_tuple])
        return select_cursor.fetchall()


def run_spending_update_query(transaction_sql, transaction_type, broker_data):
    with spending_connection.cursor() as update_cursor:
        update_query = build_spending_update_query(transaction_sql, broker_data)
        with Timer() as t:
            update_cursor.execute(update_query, [col for row in broker_data for col in row])
        row_count = update_cursor.rowcount
        logging.info("[{}] {:,} rows updated in {}".format(transaction_type, row_count, t.elapsed_as_string))
        if row_count > 0:
            now = datetime.now(timezone.utc)
            insert_values = ",".join(["({},'{}')".format(id[0], now) for id in update_cursor.fetchall()])
            update_cursor.execute(ADD_TRANSACTIONS_TO_DELTA.format(insert_values))
            logging.info("[{}] {:,} rows added to transaction_delta".format(transaction_type, update_cursor.rowcount))

        return row_count


if __name__ == "__main__":
    log_format = "[%(asctime)s] [%(levelname)s] - %(message)s"
    logging.getLogger()
    logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y/%m/%d %H:%M:%S (%Z)")

    fabs_row_count, fpds_row_count = 0, 0

    with Timer() as overall_timer:
        with (
            psycopg2.connect(dsn=SPENDING_CONNECTION_STRING) as spending_connection,
            psycopg2.connect(dsn=BROKER_CONNECTION_STRING) as broker_connection,
        ):
            spending_connection.autocommit = True

            logging.info("Running FABS backfill from Broker to USAspending")
            logging.info("Finding Published_Award_Financial_Assistance_IDs for FABS...")
            with broker_connection.cursor() as cursor:
                cursor.execute(GET_FABS_IDS_WITH_EC_SQL)
                fabs_ids = tuple(id[0] for id in cursor.fetchall())
                fabs_total = len(fabs_ids)

            logging.info("Total Published_Award_Financial_Assistance_IDs: {:,}".format(fabs_total))

            with Timer() as chunk_timer:
                for i in range(0, fabs_total, CHUNK_SIZE):
                    max_index = i + CHUNK_SIZE if i + CHUNK_SIZE < fabs_total else fabs_total
                    fabs_ids_batch = tuple(fabs_ids[i:max_index])

                    logging.info("Fetching {}-{} out of {} records from broker".format(i, max_index, fabs_total))
                    broker_fabs_data = run_broker_select_query(BROKER_FABS_SELECT_SQL, fabs_ids_batch)
                    if broker_fabs_data:
                        updated_row_count = run_spending_update_query(
                            SPENDING_FABS_UPDATE_SQL, "FABS", broker_fabs_data
                        )
                        fabs_row_count += updated_row_count
                    else:
                        print_no_rows_to_update("FABS")

            logging.info(
                "Finished running FABS backfill. Took {} to update {:,} rows".format(
                    chunk_timer.elapsed_as_string, fabs_row_count
                )
            )

            logging.info("Running FPDS backfill from Broker to USAspending")
            logging.info("Finding Detached_Award_Procurement_IDs for FPDS...")
            with broker_connection.cursor() as cursor:
                cursor.execute(GET_FPDS_IDS_WITH_EC_SQL)
                fpds_ids = tuple(id[0] for id in cursor.fetchall())
                fpds_total = len(fpds_ids)

            logging.info("Total Detached_Award_Procurement_IDs: {:,}".format(fpds_total))

            with Timer() as chunk_timer:
                for i in range(0, fpds_total, CHUNK_SIZE):
                    max_index = i + CHUNK_SIZE if i + CHUNK_SIZE < fpds_total else fpds_total
                    fpds_ids_batch = tuple(fpds_ids[i:max_index])

                    logging.info("Fetching {}-{} out of {} records from broker".format(i, max_index, fpds_total))
                    broker_fpds_data = run_broker_select_query(BROKER_FPDS_SELECT_SQL, fpds_ids_batch)
                    if broker_fpds_data:
                        updated_row_count = run_spending_update_query(
                            SPENDING_FPDS_UPDATE_SQL, "FPDS", broker_fpds_data
                        )
                        fpds_row_count += updated_row_count
                    else:
                        print_no_rows_to_update("FPDS")

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
