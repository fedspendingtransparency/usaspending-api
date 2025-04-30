"""
Jira Ticket Number(s): DEV-4120

    Backfill missing place_of_performance_scope values

Expected CLI:

    $ python3 usaspending_api/database_scripts/job_archive/backfill_primary_place_of_performance_scope.py

Purpose:

    Fetch place_of_performance_scope from Broker table:
        - published_award_financial_assistance
    And populate FABS transaction table columns:
        - place_of_performance_scope
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
    place_of_performance_scope
FROM
    published_award_financial_assistance
WHERE
    published_award_financial_assistance_id BETWEEN {min} AND {max}
"""

SPENDING_FABS_UPDATE_SQL = """
UPDATE
    transaction_fabs AS fabs SET
        place_of_performance_scope = broker.place_of_performance_scope
FROM
    (VALUES {}) AS broker(
        published_award_financial_assistance_id,
        place_of_performance_scope
    )
WHERE
    fabs.published_award_financial_assistance_id = broker.published_award_financial_assistance_id
    AND (
        fabs.place_of_performance_scope IS DISTINCT FROM broker.place_of_performance_scope
    )
"""


GET_FABS_IDS_SQL = """
SELECT MIN(published_award_financial_assistance_id), MAX(published_award_financial_assistance_id)
FROM published_award_financial_assistance
WHERE place_of_performance_scope IS NOT NULL
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


def run_broker_select_query(transaction_sql, min_id, max_id):
    with broker_connection.cursor() as select_cursor:
        select_cursor.execute(transaction_sql.format(min=min_id, max=max_id))
        return select_cursor.fetchall()


def run_spending_update_query(transaction_sql, transaction_type, broker_data):
    with spending_connection.cursor() as update_cursor:
        update_query = build_spending_update_query(transaction_sql, broker_data)
        with Timer() as t:
            update_cursor.execute(update_query, [col for row in broker_data for col in row])
        row_count = update_cursor.rowcount
        logging.info("[{}] {:,} rows updated in {}".format(transaction_type, row_count, t.elapsed_as_string))
        return row_count


if __name__ == "__main__":
    log_format = "[%(asctime)s] [%(levelname)s] - %(message)s"
    logging.getLogger()
    logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y/%m/%d %H:%M:%S (%Z)")

    fabs_row_count = 0

    with Timer() as overall_timer:
        with (
            psycopg2.connect(dsn=SPENDING_CONNECTION_STRING) as spending_connection,
            psycopg2.connect(dsn=BROKER_CONNECTION_STRING) as broker_connection,
        ):
            spending_connection.autocommit = True

            logging.info("Running FABS backfill from Broker to USAspending")
            logging.info("Finding IDs for FABS...")
            with broker_connection.cursor() as cursor:
                cursor.execute(GET_FABS_IDS_SQL)
                result = cursor.fetchall()

            min_id, max_id = result[0]
            fabs_total = max_id - min_id

            logging.info(f"MIN ID: {min_id:,}, MAX ID: {max_id:,}")
            logging.info(f"Potential total IDs: {fabs_total:,}")

            with Timer() as chunk_timer:
                _min = min_id
                while _min <= max_id:
                    _max = min(_min + CHUNK_SIZE - 1, max_id)

                    logging.info("Fetching {}-{} out of {} records from broker".format(_min, _max, fabs_total))
                    broker_fabs_data = run_broker_select_query(BROKER_FABS_SELECT_SQL, min_id=_min, max_id=_max)
                    if broker_fabs_data:
                        updated_row_count = run_spending_update_query(
                            SPENDING_FABS_UPDATE_SQL, "FABS", broker_fabs_data
                        )
                        fabs_row_count += updated_row_count
                    else:
                        print_no_rows_to_update("FABS")

                    _min = _max + 1

            logging.info(
                "Finished running FABS backfill. Took {} to update {:,} rows".format(
                    chunk_timer.elapsed_as_string, fabs_row_count
                )
            )

    logging.info(
        "Finished. Overall run time to update {:,} rows: {}".format(
            fabs_row_count + fabs_row_count, overall_timer.elapsed_as_string
        )
    )
