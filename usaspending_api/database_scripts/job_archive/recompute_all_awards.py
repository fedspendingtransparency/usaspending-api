#!/usr/bin/env python3
"""
Jira Ticket Number(s): DEV-3318, DEV-3442, DEV-3443

Expected CLI:

    $ python usaspending_api/database_scripts/job_archive/recompute_all_awards.py

Purpose:

    Adding new earliest_transaction to awards
    Improved logic for determining the initial transaction of an award which is
    used as the "earliest transaction" FK. Also make a mirror alteration to
    the logic determining "latest transaction" for an award.

    All of these changes requires updating 100% of award records

"""
import argparse
import asyncio
import asyncpg
import logging
import math
import psycopg2
import re
import signal
import time

from argparse import ArgumentTypeError
from datetime import datetime, timezone, timedelta
from dateutil import parser as date_parser
from os import environ


try:
    CONNECTION_STRING = environ["DATABASE_URL"]
except Exception:
    print("SET env var DATABASE_URL!!!\nTerminating script")
    raise SystemExit(1)

# Not a complete list of signals
# NOTE: 1-15 are relatively standard on unix platforms; > 15 can change from platform to platform
BSD_SIGNALS = {
    1: "SIGHUP [1] (Hangup detected on controlling terminal or death of controlling process)",
    2: "SIGINT [2] (Interrupt from keyboard)",
    3: "SIGQUIT [3] (Quit from keyboard)",
    4: "SIGILL [4] (Illegal Instruction)",
    6: "SIGABRT [6] (Abort signal from abort(3))",
    8: "SIGFPE [8] (Floating point exception)",
    9: "SIGKILL [9] (Non-catchable, non-ignorable kill)",
    11: "SIGSEGV [11] (Invalid memory reference)",
    14: "SIGALRM [14] (Timer signal from alarm(2))",
    15: "SIGTERM [15] (Software termination signal)",
    19: "SIGSTOP [19] (Suspend process execution)",  # NOTE: 17 on Mac OSX, 19 on RHEL
    20: "SIGTSTP [20] (Interrupt from keyboard to suspend (CTRL-Z)",  # NOTE: 18 on Mac OSX, 20 on RHEL
}


_earliest_transaction_cte = str(
    "txn_earliest AS ( "
    "  SELECT DISTINCT ON (tn.award_id) "
    "    tn.award_id, "
    "    tn.id, "
    "    tn.action_date, "
    "    tn.description, "
    "    tn.period_of_performance_start_date "
    "  FROM transaction_normalized tn"
    "  WHERE tn.award_id IN ({award_ids}) "
    "  ORDER BY tn.award_id, tn.action_date ASC, tn.modification_number ASC, tn.id ASC "
    ")"
)
_latest_transaction_cte = str(
    "txn_latest AS ( "
    "  SELECT DISTINCT ON (tn.award_id) "
    "    tn.award_id, "
    "    tn.id, "
    "    tn.awarding_agency_id, "
    "    tn.action_date, "
    "    tn.funding_agency_id, "
    "    tn.last_modified_date, "
    "    tn.period_of_performance_current_end_date, "
    "    tn.place_of_performance_id, "
    "    tn.recipient_id, "
    "    tn.type, "
    "    tn.type_description, "
    "    CASE WHEN tn.type IN ('A', 'B', 'C', 'D') THEN 'contract' "
    "      WHEN tn.type IN ('02', '03', '04', '05') THEN 'grant' "
    "      WHEN tn.type in ('06', '10') THEN 'direct payment' "
    "      WHEN tn.type in ('07', '08') THEN 'loans' "
    "      WHEN tn.type = '09' THEN 'insurance' "
    "      WHEN tn.type = '11' THEN 'other' "
    "      WHEN tn.type LIKE 'IDV%%' THEN 'idv' "
    "      ELSE NULL END AS category "
    "  FROM transaction_normalized tn"
    "  WHERE tn.award_id IN ({award_ids}) "
    "  ORDER BY tn.award_id, tn.action_date DESC, tn.modification_number DESC, tn.id DESC "
    ")"
)
_aggregate_transaction_cte = str(
    "txn_totals AS ( "
    "  SELECT "
    "    award_id, "
    "    SUM(federal_action_obligation) AS total_obligation, "
    "    SUM(original_loan_subsidy_cost) AS total_subsidy_cost, "
    "    SUM(funding_amount) AS total_funding_amount, "
    "    SUM(face_value_loan_guarantee) AS total_loan_value, "
    "    SUM(non_federal_funding_amount) AS non_federal_funding_amount "
    "  FROM transaction_normalized "
    "  WHERE award_id IN ({award_ids}) "
    "  GROUP BY award_id "
    ")"
)
_executive_comp_cte = str(
    "executive_comp AS ( "
    "  SELECT DISTINCT ON (tn.award_id) "
    "    tn.award_id, "
    "    tf.officer_1_amount, "
    "    tf.officer_1_name, "
    "    tf.officer_2_amount, "
    "    tf.officer_2_name, "
    "    tf.officer_3_amount, "
    "    tf.officer_3_name, "
    "    tf.officer_4_amount, "
    "    tf.officer_4_name, "
    "    tf.officer_5_amount, "
    "    tf.officer_5_name "
    "  FROM transaction_normalized tn "
    "  INNER JOIN {transaction_table} AS tf ON tn.id = tf.transaction_id "
    "  WHERE tf.officer_1_name IS NOT NULL AND award_id IN ({award_ids}) "
    "  ORDER BY tn.award_id, tn.action_date DESC, tn.modification_number DESC, tn.id DESC "
    ") "
)

UPDATE_AWARDS_SQL = str(
    "WITH {}, {}, {}, {} "
    "UPDATE awards a "
    "SET "
    "earliest_transaction_id = e.id, "
    "date_signed = e.action_date, "
    "description = e.description, "
    "period_of_performance_start_date = e.period_of_performance_start_date, "
    ""
    "latest_transaction_id = l.id, "
    "awarding_agency_id = l.awarding_agency_id, "
    "category = l.category, "
    "certified_date = l.action_date, "
    "funding_agency_id = l.funding_agency_id, "
    "last_modified_date = l.last_modified_date, "
    "period_of_performance_current_end_date = l.period_of_performance_current_end_date, "
    "place_of_performance_id = l.place_of_performance_id, "
    "recipient_id = l.recipient_id, "
    "type = l.type, "
    "type_description = l.type_description, "
    ""
    "non_federal_funding_amount = t.non_federal_funding_amount, "
    "total_funding_amount = t.total_funding_amount, "
    "total_loan_value = t.total_loan_value, "
    "total_obligation = t.total_obligation, "
    "total_subsidy_cost = t.total_subsidy_cost, "
    ""
    "officer_1_amount = ec.officer_1_amount, "
    "officer_1_name = ec.officer_1_name, "
    "officer_2_amount = ec.officer_2_amount, "
    "officer_2_name = ec.officer_2_name, "
    "officer_3_amount = ec.officer_3_amount, "
    "officer_3_name = ec.officer_3_name, "
    "officer_4_amount = ec.officer_4_amount, "
    "officer_4_name = ec.officer_4_name, "
    "officer_5_amount = ec.officer_5_amount, "
    "officer_5_name = ec.officer_5_name "
    ""
    "FROM txn_earliest e "
    "JOIN txn_latest l ON e.award_id = l.award_id "
    "JOIN txn_totals t ON e.award_id = t.award_id "
    "LEFT JOIN executive_comp AS ec ON e.award_id = ec.award_id "
    "WHERE "
    "  a.id = e.award_id AND ("
    "    a.earliest_transaction_id IS DISTINCT FROM e.id "
    "    OR a.date_signed IS DISTINCT FROM e.action_date "
    "    OR a.description IS DISTINCT FROM e.description "
    "    OR a.period_of_performance_start_date IS DISTINCT FROM e.period_of_performance_start_date "
    "    OR a.latest_transaction_id IS DISTINCT FROM l.id "
    "    OR a.awarding_agency_id IS DISTINCT FROM l.awarding_agency_id "
    "    OR a.category IS DISTINCT FROM l.category "
    "    OR a.certified_date IS DISTINCT FROM l.action_date "
    "    OR a.funding_agency_id IS DISTINCT FROM l.funding_agency_id "
    "    OR a.last_modified_date IS DISTINCT FROM l.last_modified_date "
    "    OR a.period_of_performance_current_end_date IS DISTINCT FROM l.period_of_performance_current_end_date "
    "    OR a.place_of_performance_id IS DISTINCT FROM l.place_of_performance_id "
    "    OR a.recipient_id IS DISTINCT FROM l.recipient_id "
    "    OR a.type IS DISTINCT FROM l.type "
    "    OR a.type_description IS DISTINCT FROM l.type_description "
    "    OR a.non_federal_funding_amount IS DISTINCT FROM t.non_federal_funding_amount "
    "    OR a.total_funding_amount IS DISTINCT FROM t.total_funding_amount "
    "    OR a.total_loan_value IS DISTINCT FROM t.total_loan_value "
    "    OR a.total_obligation IS DISTINCT FROM t.total_obligation "
    "    OR a.total_subsidy_cost IS DISTINCT FROM t.total_subsidy_cost "
    "    OR a.officer_1_amount IS DISTINCT FROM ec.officer_1_amount "
    "    OR a.officer_1_name IS DISTINCT FROM ec.officer_1_name "
    "    OR a.officer_2_amount IS DISTINCT FROM ec.officer_2_amount "
    "    OR a.officer_2_name IS DISTINCT FROM ec.officer_2_name "
    "    OR a.officer_3_amount IS DISTINCT FROM ec.officer_3_amount "
    "    OR a.officer_3_name IS DISTINCT FROM ec.officer_3_name "
    "    OR a.officer_4_amount IS DISTINCT FROM ec.officer_4_amount "
    "    OR a.officer_4_name IS DISTINCT FROM ec.officer_4_name "
    "    OR a.officer_5_amount IS DISTINCT FROM ec.officer_5_amount "
    "    OR a.officer_5_name IS DISTINCT FROM ec.officer_5_name "
    "  )"
)
UPDATE_AWARDS_SQL = UPDATE_AWARDS_SQL.format(
    _earliest_transaction_cte, _latest_transaction_cte, _aggregate_transaction_cte, _executive_comp_cte
)

DEBUG, CLEANUP = False, False
GET_FABS_AWARDS = "SELECT id FROM awards where is_fpds = FALSE AND id BETWEEN {minid} AND {maxid}"
GET_FPDS_AWARDS = "SELECT id FROM awards where is_fpds = TRUE AND id BETWEEN {minid} AND {maxid}"
GET_MIN_MAX_SQL = "SELECT MIN(id), MAX(id) FROM awards"
MAX_ID, MIN_ID, CLOSING_TIME, ITERATION_ESTIMATED_SECONDS = None, None, None, None
TOTAL_UPDATES, CHUNK_SIZE = 0, 20000
EXIT_SIGNALS = [signal.SIGHUP, signal.SIGABRT, signal.SIGINT, signal.SIGQUIT, signal.SIGTERM]


def _handle_exit_signal(signum, frame):
    """ Attempt to gracefully handle the exiting of the job as a result of receiving an exit signal."""
    signal_or_human = BSD_SIGNALS.get(signum, signum)
    logging.warn("Received signal {}. Attempting to gracefully exit".format(signal_or_human))
    teardown(successful_run=False)
    raise SystemExit()


def datetime_command_line_argument_type(naive):
    # Stolen from usaspending-api/usaspending_api/common/helpers/date_helper.py
    def _datetime_command_line_argument_type(input_string):
        try:
            parsed = date_parser.parse(input_string)
            if naive:
                if parsed.tzinfo is not None:
                    parsed = parsed.astimezone(timezone.utc)
                return parsed.replace(tzinfo=None)
            else:
                if parsed.tzinfo is None:
                    return parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc)

        except (OverflowError, TypeError, ValueError):
            raise ArgumentTypeError("Unable to convert provided value to date/time")

    return _datetime_command_line_argument_type


class Timer:
    def __init__(self, msg, pipe_output=print):
        self.elapsed = None
        self.end = None
        self.msg = msg
        self.print_func = pipe_output

    def __enter__(self):
        self.start = time.perf_counter()
        self.print_func("Running   {} ...".format(self.msg))
        return self

    def __exit__(self, *args, **kwargs):
        self.end = time.perf_counter()
        self.elapsed = self.end - self.start
        self.print_func("Completed {} in {}".format(self.msg, self.pretty_print(self.elapsed)))

    @staticmethod
    def pretty_print(elapsed):
        f, s = math.modf(elapsed)
        m, s = divmod(s, 60)
        h, m = divmod(m, 60)
        return "%d:%02d:%02d.%04d" % (h, m, s, f * 10000)


async def async_run_create(sql):
    conn = await asyncpg.connect(dsn=CONNECTION_STRING)
    try:
        insert_msg = await conn.execute(sql)
        await conn.close()
    except Exception:
        logging.exception("=== ERROR ===")
        logging.error("{}\n=============".format(sql))
        raise SystemExit

    if DEBUG:
        logging.info(sql)
        logging.info(insert_msg)
    count = re.findall(r"\d+", insert_msg)  # Example msg: UPDATE 10
    return int(count[0])


def run_update_query(fabs_awards, fpds_awards):
    loop = asyncio.new_event_loop()
    statements = []
    if fabs_awards:
        fabs_sql = UPDATE_AWARDS_SQL.format(award_ids=", ".join(fabs_awards), transaction_table="transaction_fabs")
        statements.append(asyncio.ensure_future(async_run_create(fabs_sql), loop=loop))
    if fpds_awards:
        fpds_sql = UPDATE_AWARDS_SQL.format(award_ids=", ".join(fpds_awards), transaction_table="transaction_fpds")
        statements.append(asyncio.ensure_future(async_run_create(fpds_sql), loop=loop))

    all_statements = asyncio.gather(*statements)
    loop.run_until_complete(all_statements)
    loop.close()
    return sum([stmt.result() for stmt in statements])


def main():
    global TOTAL_UPDATES
    global ITERATION_ESTIMATED_SECONDS
    with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
        connection.autocommit = True
        connection.readonly = True

        if MIN_ID is None or MAX_ID is None:
            with connection.cursor() as cursor:
                logging.info("Finding min/max IDs from awards table...")
                cursor.execute(GET_MIN_MAX_SQL)
                results = cursor.fetchall()
                min_id, max_id = results[0]

        if MAX_ID is not None:
            logging.info("Using provided MAX ID {}".format(MAX_ID))
            max_id = MAX_ID
        if MIN_ID is not None:
            logging.info("Using provided MIN ID {}".format(MIN_ID))
            min_id = MIN_ID

        if min_id >= max_id:
            raise RuntimeError("MAX ID ({}) must be greater than MIN ID ({})".format(MAX_ID, MIN_ID))

        logging.info("Min ID: {:,}".format(min_id))
        logging.info("Max ID: {:,}".format(max_id))
        logging.info("Total in range: {:,}".format(max_id - min_id + 1))
        logging.info("Closing time: {}".format(CLOSING_TIME))

        batch_min = min_id
        iteration = 1
        while batch_min <= max_id:
            batch_max = min(batch_min + CHUNK_SIZE - 1, max_id)
            if CLOSING_TIME:
                if ITERATION_ESTIMATED_SECONDS:
                    curr_time = datetime.now(timezone.utc)
                    next_run_estimated_end_datetime = curr_time + timedelta(seconds=ITERATION_ESTIMATED_SECONDS)
                    dt_str = next_run_estimated_end_datetime.isoformat()
                    if next_run_estimated_end_datetime >= CLOSING_TIME:
                        logging.info("=> Estimated loop end datetime of: {}".format(dt_str))
                        logging.info("=> Next IDs to process: {} - {}".format(batch_min, batch_max))
                        logging.info("=> You don't have to go home, but you can't... stay...... heeeeere")
                        return
                    else:
                        logging.info("=> Expected interation duration: {} ".format(ITERATION_ESTIMATED_SECONDS))
                        logging.info("=> Continuing with an estimated loop end datetime of: {}".format(dt_str))

            with Timer("[Awards {:,} - {:,}]".format(batch_min, batch_max), pipe_output=logging.info) as t:
                with connection.cursor() as cursor:
                    cursor.execute(GET_FABS_AWARDS.format(minid=batch_min, maxid=batch_max))
                    fabs = [str(row[0]) for row in cursor.fetchall()]
                    cursor.execute(GET_FPDS_AWARDS.format(minid=batch_min, maxid=batch_max))
                    fpds = [str(row[0]) for row in cursor.fetchall()]

                if fabs or fpds:
                    row_count = run_update_query(fabs_awards=fabs, fpds_awards=fpds)
                    logging.info("UPDATED {:,} records".format(row_count))
                    TOTAL_UPDATES += row_count
                else:
                    logging.info("#### No awards to update in range ###")

            if ITERATION_ESTIMATED_SECONDS is None:
                ITERATION_ESTIMATED_SECONDS = t.elapsed  # get seconds from timedelta object
            else:
                ITERATION_ESTIMATED_SECONDS = rolling_average(ITERATION_ESTIMATED_SECONDS, t.elapsed, iteration)

            batch_min = batch_max + 1
            iteration += 1


def rolling_average(current_avg: float, new_value: float, total_count: int) -> float:
    """Recalculate a running (or moving) average

    Needs the current average, a new value to include, and the total samples
    """
    new_average = float(current_avg)
    new_average -= new_average / total_count
    new_average += new_value / total_count
    return new_average


def setup():
    with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            logging.info("### Disabling autovacuum ###")
            cursor.execute("ALTER TABLE awards SET (autovacuum_enabled = false, toast.autovacuum_enabled = false)")
            logging.info(cursor.statusmessage)


def teardown(successful_run=False):
    global CLEANUP
    if CLEANUP:
        return
    logging.info("Cleaning up table and restoring autovacuum")
    with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            with Timer("teardown", pipe_output=logging.info):
                if successful_run and TOTAL_UPDATES > 0:
                    logging.info("### running vacuum ###")
                    cursor.execute("VACUUM (ANALYZE, VERBOSE) awards")
                    logging.info(cursor.statusmessage)
                logging.info("### Resetting autovacuum ###")
                cursor.execute("ALTER TABLE awards SET (autovacuum_enabled = true, toast.autovacuum_enabled = true)")
                logging.info(cursor.statusmessage)

    CLEANUP = True


if __name__ == "__main__":
    log_format = "[%(asctime)s] [%(levelname)s] - %(message)s"
    logging.getLogger()
    logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y/%m/%d %H:%M:%S (%Z)")

    parser = argparse.ArgumentParser()
    parser.add_argument("--closing-time", type=datetime_command_line_argument_type(naive=False))
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)
    parser.add_argument("--max-id", type=int)
    parser.add_argument("--min-id", type=int)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    CLOSING_TIME = args.closing_time
    CHUNK_SIZE = args.chunk_size
    MAX_ID = args.max_id
    MIN_ID = args.min_id
    DEBUG = args.debug

    for sig in EXIT_SIGNALS:
        signal.signal(sig, _handle_exit_signal)

    successful_run = False
    with Timer("recompute_all_awards", pipe_output=logging.info):
        setup()
        try:
            main()
            successful_run = True
        except Exception:
            logging.exception("ERROR ENCOUNTERED!!!")
            raise SystemExit(1)
        finally:
            teardown(successful_run)

    logging.info("Finished. Updated {:,} total award records".format(TOTAL_UPDATES))
