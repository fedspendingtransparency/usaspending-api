import logging
import os
import subprocess

from django.conf import settings
from time import perf_counter, sleep

from usaspending_api.common.csv_helpers import count_rows_in_delimited_file
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.etl.elasticsearch_loader_helpers.utilities import format_log, execute_sql_statement, DataJob

logger = logging.getLogger("script")

COUNT_FY_SQL = """
SELECT COUNT(*) AS count
FROM {view}
WHERE {fiscal_year_field}={fy} AND update_date >= '{update_date}'
"""

COUNT_SQL = """
SELECT COUNT(*) AS count
FROM {view}
WHERE update_date >= '{update_date}'
"""

ID_SQL = """
SELECT {id_col}
FROM {view}
WHERE update_date >= '{update_date}'
"""

COPY_SQL = """"COPY (
    SELECT *
    FROM {view}
    WHERE {fiscal_year_field}={fy} AND update_date >= '{update_date}'
) TO STDOUT DELIMITER ',' CSV HEADER" > '{filename}'
"""

EXTRACT_SQL = """
    SELECT *
    FROM {view}
    WHERE {id_col} IN {ids}
"""

# ==============================================================================
# Other Globals
# ==============================================================================

AWARD_DESC_CATEGORIES = {
    "loans": "loans",
    "grant": "grants",
    "insurance": "other",
    "other": "other",
    "contract": "contracts",
    "direct payment": "directpayments",
}

UNIVERSAL_TRANSACTION_ID_NAME = "generated_unique_transaction_id"
UNIVERSAL_AWARD_ID_NAME = "generated_unique_award_id"


def configure_sql_strings(config, filename, deleted_ids):
    """
    Populates the formatted strings defined globally in this file to create the desired SQL
    """
    if config["load_type"] == "awards":
        view = settings.ES_AWARDS_ETL_VIEW_NAME
        fiscal_year_field = "fiscal_year"
    else:
        view = settings.ES_TRANSACTIONS_ETL_VIEW_NAME
        fiscal_year_field = "transaction_fiscal_year"

    copy_sql = COPY_SQL.format(
        fy=config["fiscal_year"],
        update_date=config["starting_date"],
        filename=filename,
        view=view,
        fiscal_year_field=fiscal_year_field,
    )

    count_sql = COUNT_FY_SQL.format(
        fy=config["fiscal_year"], update_date=config["starting_date"], view=view, fiscal_year_field=fiscal_year_field
    )

    return copy_sql, count_sql


def get_updated_record_count(config):
    if config["load_type"] == "awards":
        view_name = settings.ES_AWARDS_ETL_VIEW_NAME
    else:
        view_name = settings.ES_TRANSACTIONS_ETL_VIEW_NAME

    count_sql = COUNT_SQL.format(update_date=config["starting_date"], view=view_name)

    return execute_sql_statement(count_sql, True, config["verbose"])[0]["count"]


def obtain_all_ids_to_process(config):
    if config["load_type"] == "awards":
        view_name = settings.ES_AWARDS_ETL_VIEW_NAME
        id_col = "award_id"
    else:
        view_name = settings.ES_TRANSACTIONS_ETL_VIEW_NAME
        id_col = "transaction_id"

    sql = ID_SQL.format(update_date=config["starting_date"], view=view_name, id_col=id_col)
    return list([x[id_col] for x in execute_sql_statement(sql, True, config["verbose"])])


def download_db_records(fetch_jobs, done_jobs, config):
    # There was recurring issue with .empty() returning true when the queue
    #  actually contained multiple jobs. Potentially caused by a race condition
    #  Funny story: adding the log statement was enough to prevent the issue
    #  Decided to be safe and added short pause to guarentee no race condition
    sleep(5)
    logger.info(format_log(f"Queue has items: {not fetch_jobs.empty()}", process="Download"))
    while not fetch_jobs.empty():
        if done_jobs.full():
            logger.info(format_log(f"Paused downloading new CSVs so ES indexing can catch up", process="Download"))
            sleep(60)
        else:
            start = perf_counter()
            job = fetch_jobs.get_nowait()
            logger.info(format_log(f"Preparing to download '{job.csv}'", process="Download"))

            sql_config = {
                "starting_date": config["starting_date"],
                "fiscal_year": job.fy,
                "process_deletes": config["process_deletes"],
                "load_type": config["load_type"],
            }
            copy_sql, count_sql = configure_sql_strings(sql_config, job.csv, [])

            if os.path.isfile(job.csv):
                os.remove(job.csv)

            job.count = download_csv(count_sql, copy_sql, job.csv, job.name, config["skip_counts"], config["verbose"])
            done_jobs.put(job)
            logger.info(
                format_log(f"CSV '{job.csv}' copy took {perf_counter() - start:.2f}s", job=job.name, process="Download")
            )
            sleep(1)

    # This "Null Job" is used to notify the other (ES data load) process this is the final job
    done_jobs.put(DataJob(None, None, None, None))
    logger.info(format_log(f"PostgreSQL COPY operations complete", process="Download"))
    return


def download_csv(count_sql, copy_sql, filename, job_id, skip_counts, verbose):

    # Execute Copy SQL to download records to CSV
    # It is preferable to not use shell=True, but this command works. Limited user-input so risk is low
    subprocess.Popen(f"psql {get_database_dsn_string()} -c {copy_sql}", shell=True).wait()
    download_count = count_rows_in_delimited_file(filename, has_header=True, safe=False)
    logger.info(format_log(f"Wrote {download_count:,} to this file: {filename}", job=job_id, process="Download"))

    # If --skip_counts is disabled, execute count_sql and compare this count to the download_count
    if not skip_counts:
        sql_count = execute_sql_statement(count_sql, True, verbose)[0]["count"]
        if sql_count != download_count:
            msg = f'Mismatch between CSV "{filename}" and DB!!! Expected: {sql_count:,} | Actual: {download_count:,}'
            logger.error(format_log(msg, job=job_id, process="Download"))
            raise SystemExit(1)
    else:
        logger.info(format_log(f"Skipping count comparison checks (sql vs download)", job=job_id, process="Download"))

    return download_count
