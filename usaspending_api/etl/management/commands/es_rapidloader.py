import json
import logging
import os
import pandas as pd
import subprocess

from collections import defaultdict
from datetime import datetime, timezone
from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand
from elasticsearch import helpers, TransportError, Elasticsearch
from elasticsearch_dsl import Search, Q as ES_Q
from multiprocessing import Process, Queue
from pathlib import Path
from time import perf_counter, sleep
from typing import Optional
from typing import Tuple

from usaspending_api.awards.v2.lookups.elasticsearch_lookups import INDEX_ALIASES_TO_AWARD_TYPES
from usaspending_api.broker.helpers.last_load_date import get_last_load_date
from usaspending_api.broker.helpers.last_load_date import update_last_load_date
from usaspending_api.common.csv_helpers import count_rows_in_delimited_file
from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client
from usaspending_api.common.elasticsearch.elasticsearch_sql_helpers import ensure_view_exists
from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type, fy as parse_fiscal_year
from usaspending_api.common.helpers.fiscal_year_helpers import create_fiscal_year_list
from usaspending_api.common.helpers.s3_helpers import retrieve_s3_bucket_object_list, access_s3_object
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.etl.elasticsearch_loader_helpers import (
    check_awards_for_deletes,
    chunks,
    execute_sql_statement,
)


logger = logging.getLogger("script")


class Command(BaseCommand):
    """ETL script for indexing transaction data into Elasticsearch

    HIGHLEVEL PROCESS OVERVIEW
         1. Generate the full list of fiscal years to process as jobs
         2. Iterate by job
           a. Download a CSV file by year (one at a time)
               i. Continue to download a CSV file until all years are downloaded
           b. Upload a CSV to Elasticsearch
               i. Continue to upload a CSV file until all years are uploaded to ES
           c. Delete CSV file
    TO RELOAD ALL data:
        python3 manage.py es_rapidloader --index-name <NEW-INDEX-NAME> --create-new-index all

        Running with --new-index will trigger several actions:
        0. A view will be created in the source database for the ETL queries
        1. A new index will be created from the value provided by --index-name (obviously)
        2. A new index template will be loaded into the cluster to set mapping and index metadata
        3. All aliases used by the API queries will be re-assigned to the new index
        4. An alias for incremental indexes will be applied to the new index
        5. If any previous indexes existed with the API aliases, they will be deleted.
    """

    help = """Hopefully the code comments are helpful enough to figure this out...."""

    def add_arguments(self, parser):
        parser.add_argument(
            "fiscal_years",
            nargs="+",
            type=str,
            metavar="fiscal-years",
            help="Provide a list of fiscal years to process. For convenience, provide 'all' for FY2008 to current FY",
        )
        parser.add_argument(
            "--process-deletes",
            action="store_true",
            help="When this flag is set, the script will include the process to "
            "obtain records of deleted transactions from S3 and remove from the index",
        )
        parser.add_argument(
            "--dir",
            default=str(Path(__file__).resolve().parent),
            type=str,
            help="Set for a custom location of output files",
            dest="directory",
        )
        parser.add_argument(
            "--skip-counts",
            action="store_true",
            help="When this flag is set, the ETL process will skip the record counts to reduce operation time",
        )
        parser.add_argument(
            "--index-name",
            type=str,
            help="Provide name for new index about to be created. Only used when --create-new-index is provided",
        )
        parser.add_argument(
            "--create-new-index",
            action="store_true",
            help="It needs a new unique index name and set aliases used by API logic to the new index",
        )
        parser.add_argument(
            "--snapshot",
            action="store_true",
            help="Create a new Elasticsearch snapshot of the current index state which is stored in S3",
        )
        parser.add_argument(
            "--start-datetime",
            type=datetime_command_line_argument_type(naive=False),
            help="Processes transactions updated on or after the UTC date/time provided. yyyy-mm-dd hh:mm:ss is always "
            "a safe format. Wrap in quotes if date/time contains spaces.",
        )
        parser.add_argument(
            "--skip-delete-index",
            action="store_true",
            help="When creating a new index skip the step that deletes the old indexes and swaps the aliases. "
            "Only used when --create-new-index is provided.",
        )
        parser.add_argument(
            "--load-type",
            type=str,
            help="Select which type of load to perform, current options are transactions or awards.",
            choices=["transactions", "awards"],
            default="transactions",
        )
        parser.add_argument(
            "--idle-wait-time",
            type=int,
            help="Time in seconds the ES index process should wait before looking for a new CSV data file.",
            default=60,
        )

    def handle(self, *args, **options):
        elasticsearch_client = instantiate_elasticsearch_client()
        config = process_cli_parameters(options, elasticsearch_client)

        start = perf_counter()
        logger.info(format_log(f"Starting script\n{'=' * 56}"))
        start_msg = "target index: {index_name} | FY(s): {fiscal_years} | Starting from: {starting_date}"
        logger.info(format_log(start_msg.format(**config)))

        if config["load_type"] == "transactions":
            ensure_view_exists(settings.ES_TRANSACTIONS_ETL_VIEW_NAME)
        elif config["load_type"] == "awards":
            ensure_view_exists(settings.ES_AWARDS_ETL_VIEW_NAME)

        loader = Rapidloader(config, elasticsearch_client)
        loader.run_load_steps()
        loader.complete_process()

        logger.info(format_log("---------------------------------------------------------------"))
        logger.info(format_log(f"Script completed in {perf_counter() - start:.2f}s"))
        logger.info(format_log("---------------------------------------------------------------"))


def process_cli_parameters(options: dict, es_client) -> dict:
    default_datetime = datetime.strptime(f"{settings.API_SEARCH_MIN_DATE}+0000", "%Y-%m-%d%z")
    simple_args = (
        "skip_delete_index",
        "process_deletes",
        "create_new_index",
        "snapshot",
        "index_name",
        "directory",
        "skip_counts",
        "load_type",
    )
    config = set_config(simple_args, options)

    config["fiscal_years"] = fiscal_years_for_processing(options)
    config["directory"] = Path(config["directory"]).resolve()

    if config["create_new_index"] and not config["index_name"]:
        raise SystemExit("Fatal error: --create-new-index requires --index-name.")
    elif config["create_new_index"]:
        config["index_name"] = config["index_name"].lower()
        config["starting_date"] = default_datetime
        check_new_index_name_is_ok(
            config["index_name"],
            settings.ES_AWARDS_NAME_SUFFIX if config["load_type"] == "awards" else settings.ES_TRANSACTIONS_NAME_SUFFIX,
        )
    elif options["start_datetime"]:
        config["starting_date"] = options["start_datetime"]
    else:
        # Due to the queries used for fetching postgres data,
        #  `starting_date` needs to be present and a date before:
        #      - The earliest records in S3.
        #      - When all transaction records in the USAspending SQL database were updated.
        #   And keep it timezone-award for S3
        config["starting_date"] = get_last_load_date(f"es_{options['load_type']}", default=default_datetime)

    config["max_query_size"] = settings.ES_TRANSACTIONS_MAX_RESULT_WINDOW
    if options["load_type"] == "awards":
        config["max_query_size"] = settings.ES_AWARDS_MAX_RESULT_WINDOW

    config["is_incremental_load"] = not bool(config["create_new_index"]) and (
        config["starting_date"] != default_datetime
    )

    if config["is_incremental_load"]:
        write_alias = settings.ES_TRANSACTIONS_WRITE_ALIAS
        if config["load_type"] == "awards":
            write_alias = settings.ES_AWARDS_WRITE_ALIAS
        if config["index_name"]:
            logger.info(format_log(f"Ignoring provided index name, using alias '{write_alias}' for incremental load"))
        config["index_name"] = write_alias
        if not es_client.cat.aliases(name=write_alias):
            logger.error(format_log(f"Write alias '{write_alias}' is missing"))
            raise SystemExit(1)
        # Force manual refresh for atomic transaction-like delete/re-add consistency during incremental load.
        # Turned back on at end.
        toggle_refresh_off(es_client, config["index_name"])
    else:
        if es_client.indices.exists(config["index_name"]):
            logger.error(format_log(f"Data load into existing index. Change index name or run an incremental load"))
            raise SystemExit(1)

    if not config["directory"].is_dir():
        logger.error(format_log(f"Provided directory does not exist"))
        raise SystemExit(1)
    elif config["starting_date"] < default_datetime:
        logger.error(format_log(f"--start-datetime is too early. Set no earlier than {default_datetime}"))
        raise SystemExit(1)
    elif not config["is_incremental_load"] and config["process_deletes"]:
        logger.error(format_log("Skipping deletions for ths load, --deleted overwritten to False"))
        config["process_deletes"] = False

    config["ingest_wait"] = options["idle_wait_time"]

    return config


def set_config(copy_args: list, arg_parse_options: dict) -> dict:
    """Set values based on env vars and when the script started"""
    root_index = settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX
    if arg_parse_options["load_type"] == "awards":
        root_index = settings.ES_AWARDS_QUERY_ALIAS_PREFIX
    config = {
        "aws_region": settings.USASPENDING_AWS_REGION,
        "s3_bucket": settings.DELETED_TRANSACTION_JOURNAL_FILES,
        "root_index": root_index,
        "processing_start_datetime": datetime.now(timezone.utc),
        "verbose": arg_parse_options["verbosity"] > 1,  # convert the management command's levels of verbosity to a bool
    }

    config.update({k: v for k, v in arg_parse_options.items() if k in copy_args})
    return config


def fiscal_years_for_processing(options: list) -> list:
    if "all" in options["fiscal_years"]:
        return create_fiscal_year_list(start_year=parse_fiscal_year(settings.API_SEARCH_MIN_DATE))
    return [int(x) for x in options["fiscal_years"]]


def check_new_index_name_is_ok(provided_name: str, suffix: str) -> None:
    if not provided_name.endswith(suffix):
        raise SystemExit(f"new index name doesn't end with the expected pattern: '{suffix}'")


class Rapidloader:
    def __init__(self, config, elasticsearch_client):
        """Set values based on env vars and when the script started"""
        self.config = config
        self.elasticsearch_client = elasticsearch_client

    def run_load_steps(self) -> None:
        download_queue = Queue()  # Queue for jobs which need a csv downloaded
        es_ingest_queue = Queue(20)  # Queue for jobs which have a csv and are ready for ES ingest

        updated_record_count = get_updated_record_count(self.config)
        logger.info(format_log(f"Found {updated_record_count:,} {self.config['load_type']} records to index"))

        if updated_record_count == 0:
            jobs = 0
        else:
            download_queue, jobs = self.create_download_jobs()

        logger.info(format_log(f"There are {jobs} jobs to process"))

        process_list = [
            Process(
                name="Download Process",
                target=download_db_records,
                args=(download_queue, es_ingest_queue, self.config),
            ),
            Process(
                name="ES Index Process",
                target=es_data_loader,
                args=(self.elasticsearch_client, download_queue, es_ingest_queue, self.config),
            ),
        ]

        if updated_record_count != 0:  # only run if there are data to process
            process_list[0].start()  # Start Download process

        if self.config["process_deletes"]:
            process_list.append(
                Process(
                    name="S3 Deleted Records Scrapper Process",
                    target=deleted_transactions if self.config["load_type"] == "transactions" else deleted_awards,
                    args=(self.elasticsearch_client, self.config),
                )
            )
            process_list[-1].start()  # start S3 csv fetch proces
            while process_list[-1].is_alive():
                logger.info(format_log("Waiting to start ES ingest until S3 deletes are complete"))
                sleep(7)  # add a brief pause to make sure the deletes are processed in ES

        if updated_record_count != 0:
            process_list[1].start()  # start ES ingest process

        while True:
            sleep(10)
            if process_guarddog(process_list):
                raise SystemExit("Fatal error: review logs to determine why process died.")
            elif all([not x.is_alive() for x in process_list]):
                logger.info(format_log("All ETL processes completed execution with no error codes"))
                break

    def create_download_jobs(self) -> Tuple[Queue, int]:
        download_queue = Queue()
        for job_number, fiscal_year in enumerate(self.config["fiscal_years"], start=1):
            index = self.config["index_name"]
            filename = str(self.config["directory"] / f"{fiscal_year}_{self.config['load_type']}.csv")

            new_job = DataJob(job_number, index, fiscal_year, filename)

            if Path(filename).exists():
                Path(filename).unlink()
            download_queue.put(new_job)
        return download_queue, job_number

    def complete_process(self) -> None:
        if self.config["create_new_index"]:
            set_final_index_config(self.elasticsearch_client, self.config["index_name"])
            if self.config["skip_delete_index"]:
                logger.info(format_log("Skipping deletion of old indices"))
            else:
                logger.info(format_log("Closing old indices and adding aliases"))
                swap_aliases(self.elasticsearch_client, self.config["index_name"], self.config["load_type"])

        if self.config["snapshot"]:
            logger.info(format_log("Taking snapshot"))
            take_snapshot(self.elasticsearch_client, self.config["index_name"], settings.ES_REPOSITORY)

        if self.config["is_incremental_load"]:
            toggle_refresh_on(self.elasticsearch_client, self.config["index_name"])
            logger.info(
                format_log(f"Storing datetime {self.config['processing_start_datetime']} for next incremental load")
            )
            update_last_load_date(f"es_{self.config['load_type']}", self.config["processing_start_datetime"])


VIEW_COLUMNS = [
    "transaction_id",
    "detached_award_proc_unique",
    "afa_generated_unique",
    "generated_unique_transaction_id",
    "display_award_id",
    "update_date",
    "modification_number",
    "generated_unique_award_id",
    "award_id",
    "piid",
    "fain",
    "uri",
    "award_description",
    "product_or_service_code",
    "product_or_service_description",
    "psc_agg_key",
    "naics_code",
    "naics_description",
    "naics_agg_key",
    "type_description",
    "award_category",
    "recipient_unique_id",
    "recipient_name",
    "recipient_hash",
    "recipient_agg_key",
    "parent_recipient_unique_id",
    "parent_recipient_name",
    "parent_recipient_hash",
    "action_date",
    "fiscal_action_date",
    "period_of_performance_start_date",
    "period_of_performance_current_end_date",
    "ordering_period_end_date",
    "transaction_fiscal_year",
    "award_fiscal_year",
    "award_amount",
    "transaction_amount",
    "face_value_loan_guarantee",
    "original_loan_subsidy_cost",
    "generated_pragmatic_obligation",
    "awarding_agency_id",
    "funding_agency_id",
    "awarding_toptier_agency_name",
    "funding_toptier_agency_name",
    "awarding_subtier_agency_name",
    "funding_subtier_agency_name",
    "awarding_toptier_agency_abbreviation",
    "funding_toptier_agency_abbreviation",
    "awarding_subtier_agency_abbreviation",
    "funding_subtier_agency_abbreviation",
    "awarding_toptier_agency_agg_key",
    "funding_toptier_agency_agg_key",
    "awarding_subtier_agency_agg_key",
    "funding_subtier_agency_agg_key",
    "cfda_number",
    "cfda_title",
    "type_of_contract_pricing",
    "type_set_aside",
    "extent_competed",
    "type",
    "pop_country_code",
    "pop_country_name",
    "pop_state_code",
    "pop_county_code",
    "pop_county_name",
    "pop_zip5",
    "pop_congressional_code",
    "pop_city_name",
    "pop_county_agg_key",
    "pop_congressional_agg_key",
    "pop_state_agg_key",
    "pop_country_agg_key",
    "recipient_location_country_code",
    "recipient_location_country_name",
    "recipient_location_state_code",
    "recipient_location_county_code",
    "recipient_location_county_name",
    "recipient_location_zip5",
    "recipient_location_congressional_code",
    "recipient_location_city_name",
    "recipient_location_county_agg_key",
    "recipient_location_congressional_agg_key",
    "recipient_location_state_agg_key",
    "tas_paths",
    "tas_components",
    "federal_accounts",
    "business_categories",
    "disaster_emergency_fund_codes",
]
AWARD_VIEW_COLUMNS = [
    "award_id",
    "generated_unique_award_id",
    "display_award_id",
    "category",
    "type",
    "type_description",
    "piid",
    "fain",
    "uri",
    "total_obligation",
    "description",
    "award_amount",
    "total_subsidy_cost",
    "total_loan_value",
    "update_date",
    "recipient_name",
    "recipient_hash",
    "recipient_agg_key",
    "recipient_unique_id",
    "parent_recipient_unique_id",
    "business_categories",
    "action_date",
    "fiscal_year",
    "last_modified_date",
    "period_of_performance_start_date",
    "period_of_performance_current_end_date",
    "date_signed",
    "ordering_period_end_date",
    "original_loan_subsidy_cost",
    "face_value_loan_guarantee",
    "awarding_agency_id",
    "funding_agency_id",
    "awarding_toptier_agency_name",
    "funding_toptier_agency_name",
    "awarding_subtier_agency_name",
    "funding_subtier_agency_name",
    "awarding_toptier_agency_code",
    "funding_toptier_agency_code",
    "awarding_subtier_agency_code",
    "funding_subtier_agency_code",
    "funding_toptier_agency_agg_key",
    "funding_subtier_agency_agg_key",
    "recipient_location_country_code",
    "recipient_location_country_name",
    "recipient_location_state_code",
    "recipient_location_county_code",
    "recipient_location_county_name",
    "recipient_location_congressional_code",
    "recipient_location_zip5",
    "recipient_location_city_name",
    "recipient_location_county_agg_key",
    "recipient_location_congressional_agg_key",
    "recipient_location_state_agg_key",
    "pop_country_code",
    "pop_country_name",
    "pop_state_code",
    "pop_county_code",
    "pop_county_name",
    "pop_zip5",
    "pop_congressional_code",
    "pop_city_name",
    "pop_city_code",
    "pop_county_agg_key",
    "pop_congressional_agg_key",
    "pop_state_agg_key",
    "cfda_number",
    "cfda_title",
    "sai_number",
    "type_of_contract_pricing",
    "extent_competed",
    "type_set_aside",
    "product_or_service_code",
    "product_or_service_description",
    "naics_code",
    "naics_description",
    "tas_paths",
    "tas_components",
    "disaster_emergency_fund_codes",
    "total_covid_obligation",
    "total_covid_outlay",
]

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

COPY_SQL = """"COPY (
    SELECT *
    FROM {view}
    WHERE {fiscal_year_field}={fy} AND update_date >= '{update_date}'
) TO STDOUT DELIMITER ',' CSV HEADER" > '{filename}'
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


class DataJob:
    def __init__(self, *args):
        self.name = args[0]
        self.index = args[1]
        self.fy = args[2]
        self.csv = args[3]
        self.count = None


def convert_postgres_array_as_string_to_list(array_as_string: str) -> Optional[list]:
    """
        Postgres arrays are stored in CSVs as strings. Elasticsearch is able to handle lists of items, but needs to
        be passed a list instead of a string. In the case of an empty array, return null.
        For example, "{this,is,a,postgres,array}" -> ["this", "is", "a", "postgres", "array"].
    """
    return array_as_string[1:-1].split(",") if len(array_as_string) > 2 else None


def convert_postgres_json_array_as_string_to_list(json_array_as_string: str) -> Optional[dict]:
    """
        Postgres JSON arrays (jsonb) are stored in CSVs as strings. Since we want to avoid nested types
        in Elasticsearch the JSON arrays are converted to dictionaries to make parsing easier and then
        converted back into a formatted string.
    """
    if json_array_as_string is None or len(json_array_as_string) == 0:
        return None
    result = []
    json_array = json.loads(json_array_as_string)
    for j in json_array:
        for key, value in j.items():
            j[key] = "" if value is None else str(j[key])
        result.append(json.dumps(j, sort_keys=True))
    return result


def process_guarddog(process_list):
    """
        pass in a list of multiprocess Process objects.
        If one errored then terminate the others and return True
    """
    for proc in process_list:
        # If exitcode is None, process is still running. exit code 0 is normal
        if proc.exitcode not in (None, 0):
            msg = f"Script proccess failed!!! {proc.name} exited with error {proc.exitcode}. Terminating all processes."
            logger.error(format_log(msg))
            [x.terminate() for x in process_list]
            return True
    return False


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


def csv_chunk_gen(filename, chunksize, job_id, load_type):
    logger.info(format_log(f"Opening {filename} (batch size = {chunksize:,})", job=job_id, process="ES Index"))
    # Need a specific converter to handle converting strings to correct data types (e.g. string -> array)
    converters = {
        "business_categories": convert_postgres_array_as_string_to_list,
        "tas_paths": convert_postgres_array_as_string_to_list,
        "tas_components": convert_postgres_array_as_string_to_list,
        "federal_accounts": convert_postgres_json_array_as_string_to_list,
        "disaster_emergency_fund_codes": convert_postgres_array_as_string_to_list,
    }
    # Panda's data type guessing causes issues for Elasticsearch. Explicitly cast using dictionary
    column_list = AWARD_VIEW_COLUMNS if load_type == "awards" else VIEW_COLUMNS
    dtype = {k: str for k in column_list if k not in converters}
    for file_df in pd.read_csv(filename, dtype=dtype, converters=converters, header=0, chunksize=chunksize):
        file_df = file_df.where(cond=(pd.notnull(file_df)), other=None)
        # Route all documents with the same recipient to the same shard
        # This allows for accuracy and early-termination of "top N" recipient category aggregation queries
        # Recipient is are highest-cardinality category with over 2M unique values to aggregate against,
        # and this is needed for performance
        # ES helper will pop any "meta" fields like "routing" from provided data dict and use them in the action
        file_df["routing"] = file_df[settings.ES_ROUTING_FIELD]

        # Explicitly setting the ES _id field to match the postgres PK value allows
        # bulk index operations to be upserts without creating duplicate documents
        file_df["_id"] = file_df[f"{'award' if load_type == 'awards' else 'transaction'}_id"]
        yield file_df.to_dict(orient="records")


def es_data_loader(client, fetch_jobs, done_jobs, config):
    if config["create_new_index"]:
        # ensure template for index is present and the latest version
        call_command("es_configure", "--template-only", f"--load-type={config['load_type']}")
    while True:
        if not done_jobs.empty():
            job = done_jobs.get_nowait()
            if job.name is None:
                break

            logger.info(format_log(f"Starting new job", job=job.name, process="ES Index"))
            post_to_elasticsearch(client, job, config)
            if os.path.exists(job.csv):
                os.remove(job.csv)
        else:
            logger.info(format_log(f"No Job. Sleeping {config['ingest_wait']}s", process="ES Index"))
            sleep(int(config["ingest_wait"]))

    logger.info(format_log(f"Completed Elasticsearch data load", process="ES Index"))
    return


def streaming_post_to_es(
    client, chunk, index_name: str, type: str, job_id=None, delete_before_index=True, delete_key="_id"
):
    """
    Called this repeatedly with successive chunks of data to pump into an Elasticsearch index.

    Args:
        client: Elasticsearch client
        chunk (List[dict]): list of dictionary objects holding field_name:value data
        index_name (str): name of targetted index
        type (str): indexed data type (e.g. awards or transactions)
        job_id (str): name of ES ETL job being run, used in logging
        delete_before_index (bool): When true, attempts to delete given documents by a unique key before indexing them.
            NOTE: For incremental loads, we must "delete-before-index" due to the fact that on many of our indices,
                we have different values for _id and routing key.
                Not doing this exposed a bug in our approach to expedite incremental UPSERTS aimed at allowing ES to
                overwrite documents when it encountered one already existing by a given _id. The problem is that the
                index operation uses the routing key to target only 1 shard for its index/overwrite. If the routing key
                value changes between two incremental loads of the same doc with the same _id, it may get routed to a
                different shard and won't overwrite the original doc, leaving duplicates across all shards in the index.
        delete_key (str): The column (field) name used for value lookup in the given chunk to derive documents to be
            deleted, if delete_before_index is True. Currently defaulting to "_id", taking advantage of the fact
            that we are explicitly setting "_id" in the documents to-be-indexed, which is a unique key for each doc
            (e.g. the PK of the DB row)

    Returns: (succeeded, failed) tuple, which counts successful index doc writes vs. failed doc writes
    """
    success, failed = 0, 0
    try:
        if delete_before_index:
            value_list = [doc[delete_key] for doc in chunk]
            delete_docs_by_unique_key(client, delete_key, value_list, job_id, index_name)
        for ok, item in helpers.parallel_bulk(client, chunk, index=index_name):
            success = [success, success + 1][ok]
            failed = [failed + 1, failed][ok]

    except Exception as e:
        logger.exception(f"Fatal error: \n\n{str(e)[:5000]}...\n\n{'*' * 80}")
        raise SystemExit(1)

    logger.info(format_log(f"Success: {success:,} | Fail: {failed:,}", job=job_id, process="ES Index"))
    return success, failed


def put_alias(client, index, alias_name, alias_body):
    client.indices.put_alias(index, alias_name, body=alias_body)


def create_aliases(client, index, load_type, silent=False):
    for award_type, award_type_codes in INDEX_ALIASES_TO_AWARD_TYPES.items():
        if load_type == "awards":
            prefix = settings.ES_AWARDS_QUERY_ALIAS_PREFIX
        else:
            prefix = settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX

        alias_name = f"{prefix}-{award_type}"
        if silent is False:
            logger.info(
                format_log(
                    f"Putting alias '{alias_name}' on {index} with award codes {award_type_codes}",
                    process="ES Alias Put",
                )
            )
        alias_body = {"filter": {"terms": {"type": award_type_codes}}}
        put_alias(client, index, alias_name, alias_body)

    # ensure the new index is added to the alias used for incremental loads.
    # If the alias is on multiple indexes, the loads will fail!
    write_alias = settings.ES_AWARDS_WRITE_ALIAS if load_type == "awards" else settings.ES_TRANSACTIONS_WRITE_ALIAS
    logger.info(format_log(f"Putting alias '{write_alias}' on {index}", process="ES Alias Put"))
    put_alias(
        client, index, write_alias, {},
    )


def set_final_index_config(client, index):
    es_settingsfile = str(settings.APP_DIR / "etl" / "es_config_objects.json")
    with open(es_settingsfile) as f:
        settings_dict = json.load(f)
    final_index_settings = settings_dict["final_index_settings"]

    current_settings = client.indices.get(index)[index]["settings"]["index"]

    client.indices.put_settings(final_index_settings, index)
    client.indices.refresh(index)
    for setting, value in final_index_settings.items():
        message = f'Changed "{setting}" from {current_settings.get(setting)} to {value}'
        logger.info(format_log(message, process="ES Settings"))


def toggle_refresh_off(client, index):
    client.indices.put_settings({"refresh_interval": "-1"}, index)
    message = (
        f'Set "refresh_interval": "-1" to turn auto refresh off during incremental load. Manual refreshes will '
        f"occur for each batch completion."
    )
    logger.info(format_log(message, process="ES Settings"))


def toggle_refresh_on(client, index):
    response = client.indices.get(index)
    aliased_index_name = list(response.keys())[0]
    current_refresh_interval = response[aliased_index_name]["settings"]["index"]["refresh_interval"]
    es_settingsfile = str(settings.APP_DIR / "etl" / "es_config_objects.json")
    with open(es_settingsfile) as f:
        settings_dict = json.load(f)
    final_refresh_interval = settings_dict["final_index_settings"]["refresh_interval"]
    client.indices.put_settings({"refresh_interval": final_refresh_interval}, index)
    message = f'Changed "refresh_interval" from {current_refresh_interval} to {final_refresh_interval}'
    logger.info(format_log(message, process="ES Settings"))


def swap_aliases(client, index, load_type):
    if client.indices.get_alias(index, "*"):
        logger.info(format_log(f"Removing old aliases for index '{index}'", process="ES Alias Drop"))
        client.indices.delete_alias(index, "_all")
    if load_type == "awards":
        alias_patterns = settings.ES_AWARDS_QUERY_ALIAS_PREFIX + "*"
    else:
        alias_patterns = settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX + "*"
    old_indexes = []

    try:
        old_indexes = list(client.indices.get_alias("*", alias_patterns).keys())
        for old_index in old_indexes:
            client.indices.delete_alias(old_index, "_all")
            logger.info(format_log(f"Removing aliases from '{old_index}'", process="ES Alias Drop"))
    except Exception:
        logger.exception(format_log(f"No aliases found for {alias_patterns}", process="ES Alias Drop"))

    create_aliases(client, index, load_type=load_type)

    try:
        if old_indexes:
            client.indices.delete(index=old_indexes, ignore_unavailable=False)
            logger.info(format_log(f"Deleted index(es) '{old_indexes}'", process="ES Alias Drop"))
    except Exception:
        logger.exception(format_log(f"Unable to delete indexes: {old_indexes}", process="ES Alias Drop"))


def post_to_elasticsearch(client, job, config, chunksize=250000):
    logger.info(format_log(f"Populating ES Index '{job.index}'", job=job.name, process="ES Index"))
    start = perf_counter()
    try:
        does_index_exist = client.indices.exists(job.index)
    except Exception as e:
        print(e)
        raise SystemExit(1)
    if not does_index_exist:
        logger.info(format_log(f"Creating index '{job.index}'", job=job.name, process="ES Index"))
        client.indices.create(index=job.index)
        client.indices.refresh(job.index)

    csv_generator = csv_chunk_gen(job.csv, chunksize, job.name, config["load_type"])
    for count, chunk in enumerate(csv_generator):
        if len(chunk) == 0:
            logger.info(format_log(f"No documents to add/delete for chunk #{count}", job=job.name, process="ES Index"))
            continue

        # Only delete before adding/inserting/indexing new docs on incremental loads, not full reindexes
        is_incremental = config["is_incremental_load"] and str(config["is_incremental_load"]).lower() == "true"

        iteration = perf_counter()
        current_rows = f"({count * chunksize + 1:,}-{count * chunksize + len(chunk):,})"
        logger.info(
            format_log(f"ES Stream #{count} rows [{current_rows}/{job.count:,}]", job=job.name, process="ES Index")
        )
        streaming_post_to_es(
            client, chunk, job.index, config["load_type"], job.name, delete_before_index=is_incremental
        )
        if is_incremental:
            # refresh_interval is off during incremental loads.
            # Manually refresh after delete + insert complete for search consistency
            client.indices.refresh(job.index)
        logger.info(
            format_log(
                f"Iteration group #{count} took {perf_counter() - iteration:.2f}s", job=job.name, process="ES Index"
            )
        )

    logger.info(
        format_log(f"Elasticsearch Index loading took {perf_counter() - start:.2f}s", job=job.name, process="ES Index")
    )


def deleted_transactions(client, config):
    deleted_ids = gather_deleted_ids(config)
    id_list = [{"key": deleted_id, "col": UNIVERSAL_TRANSACTION_ID_NAME} for deleted_id in deleted_ids]
    delete_from_es(client, id_list, None, config, None)


def deleted_awards(client, config):
    """
    so we have to find all the awards connected to these transactions,
    if we can't find the awards in the database, then we have to delete them from es
    """
    deleted_ids = gather_deleted_ids(config)
    id_list = [{"key": deleted_id, "col": UNIVERSAL_TRANSACTION_ID_NAME} for deleted_id in deleted_ids]
    award_ids = get_deleted_award_ids(client, id_list, config, settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX + "-*")
    if (len(award_ids)) == 0:
        logger.info(format_log(f"No related awards require deletion", process="ES Delete"))
        return
    deleted_award_ids = check_awards_for_deletes(award_ids)
    if len(deleted_award_ids) != 0:
        award_id_list = [
            {"key": deleted_award["generated_unique_award_id"], "col": UNIVERSAL_AWARD_ID_NAME}
            for deleted_award in deleted_award_ids
        ]
        delete_from_es(client, award_id_list, None, config, None)
    else:
        logger.info(format_log(f"No related awards require deletion", process="ES Delete"))
    return


def take_snapshot(client, index, repository):
    snapshot_name = f"{index}-{str(datetime.now().date())}"
    try:
        client.snapshot.create(repository, snapshot_name, body={"indices": index})
        logger.info(
            format_log(
                f"Taking snapshot INDEX: '{index}' SNAPSHOT: '{snapshot_name}' REPO: '{repository}'",
                process="ES Snapshot",
            )
        )
    except TransportError:
        logger.exception(format_log(f"SNAPSHOT FAILED", process="ES Snapshot"))
        raise SystemExit(1)


def gather_deleted_ids(config):
    """
    Connect to S3 and gather all of the transaction ids stored in CSV files
    generated by the broker when transactions are removed from the DB.
    """

    if not config["process_deletes"]:
        logger.info(format_log(f"Skipping the S3 CSV fetch for deleted transactions", process="ES Delete"))
        return

    logger.info(format_log(f"Gathering all deleted transactions from S3", process="ES Delete"))
    start = perf_counter()

    bucket_objects = retrieve_s3_bucket_object_list(bucket_name=config["s3_bucket"])
    logger.info(
        format_log(f"{len(bucket_objects):,} files found in bucket '{config['s3_bucket']}'", process="ES Delete")
    )

    if config["verbose"]:
        logger.info(format_log(f"CSV data from {config['starting_date']} to now", process="ES Delete"))

    filtered_csv_list = [
        x
        for x in bucket_objects
        if (x.key.endswith(".csv") and not x.key.startswith("staging") and x.last_modified >= config["starting_date"])
    ]

    if config["verbose"]:
        logger.info(format_log(f"Found {len(filtered_csv_list)} csv files", process="ES Delete"))

    deleted_ids = {}

    for obj in filtered_csv_list:
        object_data = access_s3_object(bucket_name=config["s3_bucket"], obj=obj)

        # Ingests the CSV into a dataframe. pandas thinks some ids are dates, so disable parsing
        data = pd.read_csv(object_data, dtype=str)

        if "detached_award_proc_unique" in data:
            new_ids = ["CONT_TX_" + x.upper() for x in data["detached_award_proc_unique"].values]
        elif "afa_generated_unique" in data:
            new_ids = ["ASST_TX_" + x.upper() for x in data["afa_generated_unique"].values]
        else:
            logger.info(format_log(f"[Missing valid col] in {obj.key}", process="ES Delete"))

        for uid in new_ids:
            if uid in deleted_ids:
                if deleted_ids[uid]["timestamp"] < obj.last_modified:
                    deleted_ids[uid]["timestamp"] = obj.last_modified
            else:
                deleted_ids[uid] = {"timestamp": obj.last_modified}

    if config["verbose"]:
        for uid, deleted_dict in deleted_ids.items():
            logger.info(format_log(f"id: {uid} last modified: {deleted_dict['timestamp']}", process="ES Delete"))

    logger.info(
        format_log(
            f"Gathering {len(deleted_ids):,} deleted transactions took {perf_counter() - start:.2f}s",
            process="ES Delete",
        )
    )
    return deleted_ids


def filter_query(column, values, query_type="match_phrase"):
    queries = [{query_type: {column: str(i)}} for i in values]
    return {"query": {"bool": {"should": [queries]}}}


def delete_query(response):
    return {"query": {"ids": {"values": [i["_id"] for i in response["hits"]["hits"]]}}}


def delete_from_es(client, id_list, job_id, config, index=None):
    """
    id_list = [{key:'key1',col:'tranaction_id'},
               {key:'key2',col:'generated_unique_transaction_id'}],
               ...]
    or
    id_list = [{key:'key1',col:'award_id'},
               {key:'key2',col:'generated_unique_award_id'}],
               ...]
    """
    start = perf_counter()

    logger.info(format_log(f"Deleting up to {len(id_list):,} document(s)", job=job_id, process="ES Delete"))

    if index is None:
        index = f"{config['root_index']}-*"
    start_ = client.count(index=index)["count"]
    logger.info(format_log(f"Starting amount of indices ----- {start_:,}", job=job_id, process="ES Delete"))
    col_to_items_dict = defaultdict(list)
    for l in id_list:
        col_to_items_dict[l["col"]].append(l["key"])

    for column, values in col_to_items_dict.items():
        logger.info(format_log(f"Deleting {len(values):,} of '{column}'", job=job_id, process="ES Delete"))
        values_generator = chunks(values, 1000)
        for v in values_generator:
            # IMPORTANT: This delete routine looks at just 1 index at a time. If there are duplicate records across
            # multiple indexes, those duplicates will not be caught by this routine. It is left as is because at the
            # time of this comment, we are migrating to using a single index.
            body = filter_query(column, v)
            response = client.search(index=index, body=json.dumps(body), size=config["max_query_size"])
            delete_body = delete_query(response)
            try:
                client.delete_by_query(
                    index=index, body=json.dumps(delete_body), refresh=True, size=config["max_query_size"]
                )
            except Exception:
                logger.exception(format_log(f"", job=job_id, process="ES Delete"))
                raise SystemExit(1)

    end_ = client.count(index=index)["count"]
    msg = f"ES Deletes took {perf_counter() - start:.2f}s. Deleted {start_ - end_:,} records"
    logger.info(format_log(msg, job=job_id, process="ES Delete"))
    return


def delete_docs_by_unique_key(client: Elasticsearch, key: str, value_list: list, job_id: str, index) -> int:
    """
    Bulk delete a batch of documents whose field identified by ``key`` matches any value provided in the
    ``values_list``.

    Args:
        client (Elasticsearch): elasticsearch-dsl client for making calls to an ES cluster
        key (str): name of filed in targeted elasticearch index that shoudld have a unique value for
            every doc in the index. Ideally the field or sub-field provided is of ``keyword`` type.
        value_list (list): if key field has these values, the document will be deleted
        job_id (str): name of ES ETL job being run, used in logging
        index (str): name of index (or alias) to target for the ``_delete_by_query`` ES operation.

            NOTE: This delete routine looks at just the index name given. If there are duplicate records across
            multiple indexes, an alias or wildcard should be provided for ``index`` param that covers multiple
            indices, or this will need to be run once per index.

    Returns: Number of ES documents deleted
    """
    start = perf_counter()

    logger.info(format_log(f"Deleting up to {len(value_list):,} document(s)", process="ES Delete", job=job_id))
    assert index, "index name must be provided"

    deleted = 0
    is_error = False
    try:
        # 65,536 is max number of terms that can be added to an ES terms filter query
        values_generator = chunks(value_list, 50000)
        for chunk_of_values in values_generator:
            # Creates an Elasticsearch query criteria for the _delete_by_query call
            q = ES_Q("terms", **{key: chunk_of_values})
            # Invoking _delete_by_query as per the elasticsearch-dsl docs:
            #   https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html#delete-by-query
            response = Search(using=client, index=index).filter(q).delete()
            chunk_deletes = response["deleted"]
            deleted += chunk_deletes
    except Exception:
        is_error = True
        logger.exception(format_log(f"", job=job_id, process="ES Delete"))
        raise SystemExit(1)
    finally:
        error_text = " before encountering an error" if is_error else ""
        msg = f"ES Deletes took {perf_counter() - start:.2f}s. Deleted {deleted:,} records{error_text}"
        logger.info(format_log(msg, process="ES Delete", job=job_id))

    return deleted


def get_deleted_award_ids(client, id_list, config, index=None):
    """
        id_list = [{key:'key1',col:'transaction_id'},
                   {key:'key2',col:'generated_unique_transaction_id'}],
                   ...]
     """
    if index is None:
        index = f"{config['root_index']}-*"
    col_to_items_dict = defaultdict(list)
    for l in id_list:
        col_to_items_dict[l["col"]].append(l["key"])
    awards = []
    for column, values in col_to_items_dict.items():
        values_generator = chunks(values, 1000)
        for v in values_generator:
            body = filter_query(column, v)
            response = client.search(index=index, body=json.dumps(body), size=config["max_query_size"])
            if response["hits"]["total"]["value"] != 0:
                awards = [x["_source"]["generated_unique_award_id"] for x in response["hits"]["hits"]]
    return awards


def format_log(msg, process=None, job=None):
    inner_str = f"[{process if process else 'main'}] {f'(#{job})' if job else ''}"
    return f"{inner_str:<18} | {msg}"
