#!/usr/bin/env python3
"""
Jira Ticket Number(s): DEV-3318, DEV-3442, DEV-3443, DEV-4109

Expected CLI:

    $ python3 usaspending_api/database_scripts/job_archive/recompute_all_awards.py

Purpose:

    Re-calculate all award fields to ensure the awards are accurate and up-to-date
    Can choose a range of award.id values to process, can also limit to FABS or FPDS
    for subset update runs.

    Same SQL used in usaspending_api/etl/award_helpers.py


"""
import argparse
import asyncio
import asyncpg
import logging
import math
import psycopg2
import re
import signal
import sys
import time

from pathlib import Path
from datetime import datetime, timezone, timedelta
from dateutil import parser as date_parser
from os import environ

sys.path.append(str(Path(__file__).resolve().parent.parent.parent / "etl"))
from award_helpers import (  # noqa: E402
    general_award_update_sql_string,
    fpds_award_update_sql_string,
    fabs_award_update_sql_string,
)

try:
    CONNECTION_STRING = environ["DATABASE_URL"]
except Exception:
    print("SET env var DATABASE_URL!!!\nTerminating script")
    raise SystemExit(100)

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

DEBUG, CLEANUP = False, False
GET_FABS_AWARDS = "SELECT id FROM vw_awards where is_fpds = FALSE AND id BETWEEN {minid} AND {maxid}"
GET_FPDS_AWARDS = "SELECT id FROM vw_awards where is_fpds = TRUE AND id BETWEEN {minid} AND {maxid}"
GET_MIN_MAX_SQL = "SELECT MIN(id), MAX(id) FROM vw_awards"
MAX_ID, MIN_ID, CLOSING_TIME, ITERATION_ESTIMATED_SECONDS = None, None, None, None
TOTAL_UPDATES, CHUNK_SIZE = 0, 20000
TYPES = ("fpds", "fabs")
EXIT_SIGNALS = [signal.SIGHUP, signal.SIGABRT, signal.SIGINT, signal.SIGQUIT, signal.SIGTERM]


def _handle_exit_signal(signum, frame):
    """Attempt to gracefully handle the exiting of the job as a result of receiving an exit signal."""
    signal_or_human = BSD_SIGNALS.get(signum, signum)
    logging.warning("Received signal {}. Attempting to gracefully exit".format(signal_or_human))
    teardown(successful_run=False)
    raise SystemExit(3)


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
            raise argparse.ArgumentTypeError("Unable to convert provided value to date/time")

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
        raise SystemExit(1)

    if DEBUG:
        logging.info(sql)
        logging.info(insert_msg)
    count = re.findall(r"\d+", insert_msg)  # Example msg: UPDATE 10
    return int(count[0])


def run_update_query(fabs_awards, fpds_awards):
    loop = asyncio.new_event_loop()
    statements = []

    predicate = f"WHERE tn.award_id IN ({','.join(fabs_awards + fpds_awards)})"
    all_sql = general_award_update_sql_string.format(predicate=predicate)
    statements.append(asyncio.ensure_future(async_run_create(all_sql), loop=loop))

    if fabs_awards:
        predicate = f"WHERE tn.award_id IN ({','.join(fabs_awards)})"
        fabs_sql = fabs_award_update_sql_string.format(predicate=predicate)
        statements.append(asyncio.ensure_future(async_run_create(fabs_sql), loop=loop))
    if fpds_awards:
        predicate = f"WHERE tn.award_id IN ({','.join(fpds_awards)})"
        fpds_sql = fpds_award_update_sql_string.format(predicate=predicate)
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
                    logging.info("=> Expected iteration duration: {} ".format(ITERATION_ESTIMATED_SECONDS))
                    logging.info("=> Estimated loop end datetime of: {}".format(dt_str))
                    if next_run_estimated_end_datetime >= CLOSING_TIME:
                        logging.info("===== Suspending job due to --closing-time flag")
                        logging.info("===== Start next job at ID {} =====".format(batch_min))
                        return

            with Timer("[Awards {:,} - {:,}]".format(batch_min, batch_max), pipe_output=logging.info) as t:
                with connection.cursor() as cursor:
                    if "fabs" in TYPES:
                        cursor.execute(GET_FABS_AWARDS.format(minid=batch_min, maxid=batch_max))
                        fabs = [f"'{row[0]}'" for row in cursor.fetchall()]
                    else:
                        fabs = []

                    if "fpds" in TYPES:
                        cursor.execute(GET_FPDS_AWARDS.format(minid=batch_min, maxid=batch_max))
                        fpds = [f"'{row[0]}'" for row in cursor.fetchall()]
                    else:
                        fpds = []

                if fabs or fpds:
                    row_count = run_update_query(fabs_awards=fabs, fpds_awards=fpds)
                    logging.info("UPDATED {:,} records".format(row_count))
                    TOTAL_UPDATES += row_count
                else:
                    logging.info("#### No awards to update in range ###")

            if ITERATION_ESTIMATED_SECONDS is None:
                ITERATION_ESTIMATED_SECONDS = t.elapsed
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
    parser.add_argument("--type", type=str, choices=TYPES)
    parser.add_argument("--max-id", type=int)
    parser.add_argument("--min-id", type=int)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    CLOSING_TIME = args.closing_time
    CHUNK_SIZE = args.chunk_size
    MAX_ID = args.max_id
    MIN_ID = args.min_id
    DEBUG = args.debug
    if args.type:
        TYPES = (args.type,)

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
