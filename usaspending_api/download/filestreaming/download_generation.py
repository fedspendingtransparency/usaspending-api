import json
import logging
import multiprocessing
import os
from pathlib import Path
from typing import Optional, Tuple, List

import psutil as ps
import re
import shutil
import subprocess
import tempfile
import time
import traceback

from datetime import datetime, timezone
from django.conf import settings

from usaspending_api.awards.v2.filters.filter_helpers import add_date_range_comparison_types
from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping, assistance_type_mapping, idv_type_mapping
from usaspending_api.common.csv_helpers import count_rows_in_delimited_file, partition_large_delimited_file
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.orm_helpers import generate_raw_quoted_query
from usaspending_api.common.helpers.s3_helpers import multipart_upload
from usaspending_api.common.helpers.text_helpers import slugify_text_for_file_names
from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri
from usaspending_api.download.download_utils import construct_data_date_range
from usaspending_api.download.filestreaming import NAMING_CONFLICT_DISCRIMINATOR
from usaspending_api.download.filestreaming.download_source import DownloadSource
from usaspending_api.download.filestreaming.file_description import build_file_description, save_file_description
from usaspending_api.download.filestreaming.zip_file import append_files_to_zip_file
from usaspending_api.download.helpers import verify_requested_columns_available, write_to_download_log as write_to_log
from usaspending_api.download.lookups import JOB_STATUS_DICT, VALUE_MAPPINGS, FILE_FORMATS
from usaspending_api.download.models import DownloadJob

DOWNLOAD_VISIBILITY_TIMEOUT = 60 * 10
MAX_VISIBILITY_TIMEOUT = 60 * 60 * settings.DOWNLOAD_DB_TIMEOUT_IN_HOURS
EXCEL_ROW_LIMIT = 1000000
WAIT_FOR_PROCESS_SLEEP = 5

logger = logging.getLogger(__name__)


def generate_download(download_job: DownloadJob, origination: Optional[str] = None):
    """Create data archive files from the download job object"""

    # Parse data from download_job
    json_request = json.loads(download_job.json_request)
    columns = json_request.get("columns", None)
    limit = json_request.get("limit", None)
    piid = json_request.get("piid", None)
    award_id = json_request.get("award_id")
    assistance_id = json_request.get("assistance_id")
    file_format = json_request.get("file_format")

    file_name = start_download(download_job)
    working_dir = None
    try:
        # Create temporary files and working directory
        zip_file_path = settings.CSV_LOCAL_PATH + file_name
        if not settings.IS_LOCAL and os.path.exists(zip_file_path):
            # Clean up a zip file that might exist from a prior attempt at this download
            os.remove(zip_file_path)
        working_dir = os.path.splitext(zip_file_path)[0]
        if not os.path.exists(working_dir):
            os.mkdir(working_dir)

        write_to_log(message=f"Generating {file_name}", download_job=download_job)

        # Generate sources from the JSON request object
        sources = get_download_sources(json_request, origination)
        for source in sources:
            # Parse and write data to the file; if there are no matching columns for a source then add an empty file
            source_column_count = len(source.columns(columns))
            if source_column_count == 0:
                create_empty_data_file(
                    source, download_job, working_dir, piid, assistance_id, zip_file_path, file_format
                )
            else:
                download_job.number_of_columns += source_column_count
                parse_source(
                    source, columns, download_job, working_dir, piid, assistance_id, zip_file_path, limit, file_format
                )
        include_data_dictionary = json_request.get("include_data_dictionary")
        if include_data_dictionary:
            add_data_dictionary_to_zip(working_dir, zip_file_path)
        include_file_description = json_request.get("include_file_description")
        if include_file_description:
            write_to_log(message="Adding file description to zip file")
            file_description = build_file_description(include_file_description["source"], sources)
            file_description = file_description.replace("[AWARD_ID]", str(award_id))
            file_description_path = save_file_description(
                working_dir, include_file_description["destination"], file_description
            )
            append_files_to_zip_file([file_description_path], zip_file_path)
        download_job.file_size = os.stat(zip_file_path).st_size
    except InvalidParameterException as e:
        exc_msg = "InvalidParameterException was raised while attempting to process the DownloadJob"
        fail_download(download_job, e, exc_msg)
        raise InvalidParameterException(e)
    except Exception as e:
        # Set error message; job_status_id will be set in download_sqs_worker.handle()
        exc_msg = "An exception was raised while attempting to process the DownloadJob"
        fail_download(download_job, e, exc_msg)
        raise Exception(download_job.error_message) from e
    finally:
        # Remove working directory
        if working_dir and os.path.exists(working_dir):
            shutil.rmtree(working_dir)
        _kill_spawned_processes(download_job)

    try:
        # push file to S3 bucket, if not local
        if not settings.IS_LOCAL:
            bucket = settings.BULK_DOWNLOAD_S3_BUCKET_NAME
            region = settings.USASPENDING_AWS_REGION
            start_uploading = time.perf_counter()
            multipart_upload(bucket, region, zip_file_path, os.path.basename(zip_file_path))
            write_to_log(
                message=f"Uploading took {time.perf_counter() - start_uploading:.2f}s", download_job=download_job
            )
    except Exception as e:
        # Set error message; job_status_id will be set in download_sqs_worker.handle()
        exc_msg = "An exception was raised while attempting to upload the file"
        fail_download(download_job, e, exc_msg)
        if isinstance(e, InvalidParameterException):
            raise InvalidParameterException(e)
        else:
            raise Exception(download_job.error_message) from e
    finally:
        # Remove generated file
        if not settings.IS_LOCAL and os.path.exists(zip_file_path):
            os.remove(zip_file_path)
        _kill_spawned_processes(download_job)

    return finish_download(download_job)


def get_download_sources(json_request: dict, origination: Optional[str] = None):
    download_sources = []
    for download_type in json_request["download_types"]:
        agency_id = json_request.get("agency", "all")
        filter_function = VALUE_MAPPINGS[download_type]["filter_function"]
        download_type_table = VALUE_MAPPINGS[download_type]["table"]

        if VALUE_MAPPINGS[download_type]["source_type"] == "award":
            # Award downloads

            # Use correct date range columns for advanced search
            # (Will not change anything for keyword search since "time_period" is not provided))
            filters = add_date_range_comparison_types(
                json_request["filters"],
                is_subaward=download_type != "awards",
                gte_date_type="action_date",
                lte_date_type="date_signed",
            )

            queryset = filter_function(filters)
            if filters.get("prime_and_sub_award_types") is not None:
                award_type_codes = set(filters["prime_and_sub_award_types"][download_type])
            else:
                award_type_codes = set(filters["award_type_codes"])

            if (
                award_type_codes & (set(contract_type_mapping.keys()) | set(idv_type_mapping.keys()))
                or "procurement" in award_type_codes
            ):
                # only generate d1 files if the user is asking for contract data
                d1_source = DownloadSource(VALUE_MAPPINGS[download_type]["table_name"], "d1", download_type, agency_id)
                d1_filters = {f"{VALUE_MAPPINGS[download_type]['contract_data']}__isnull": False}
                d1_source.queryset = queryset & download_type_table.objects.filter(**d1_filters)
                download_sources.append(d1_source)

            if award_type_codes & set(assistance_type_mapping.keys()) or ("grant" in award_type_codes):
                # only generate d2 files if the user is asking for assistance data
                d2_source = DownloadSource(VALUE_MAPPINGS[download_type]["table_name"], "d2", download_type, agency_id)
                d2_filters = {f"{VALUE_MAPPINGS[download_type]['assistance_data']}__isnull": False}
                d2_source.queryset = queryset & download_type_table.objects.filter(**d2_filters)
                download_sources.append(d2_source)

        elif VALUE_MAPPINGS[download_type]["source_type"] == "account":
            # Account downloads
            account_source = DownloadSource(
                VALUE_MAPPINGS[download_type]["table_name"], json_request["account_level"], download_type, agency_id
            )
            account_source.queryset = filter_function(
                download_type,
                VALUE_MAPPINGS[download_type]["table"],
                json_request["filters"],
                json_request["account_level"],
            )
            download_sources.append(account_source)

        elif VALUE_MAPPINGS[download_type]["source_type"] == "disaster":
            # Disaster Page downloads
            disaster_source = DownloadSource(
                VALUE_MAPPINGS[download_type]["source_type"],
                VALUE_MAPPINGS[download_type]["table_name"],
                download_type,
                agency_id,
            )
            disaster_source.award_category = json_request["award_category"]
            disaster_source.queryset = filter_function(
                json_request["filters"], download_type, VALUE_MAPPINGS[download_type]["base_fields"]
            )
            download_sources.append(disaster_source)

    verify_requested_columns_available(tuple(download_sources), json_request.get("columns", []))

    return download_sources


def build_data_file_name(source, download_job, piid, assistance_id):
    d_map = {"d1": "Contracts", "d2": "Assistance", "treasury_account": "TAS", "federal_account": "FA"}

    if download_job and download_job.monthly_download:
        # For monthly archives, use the existing detailed zip filename for the data files
        # e.g. FY(All)-012_Contracts_Delta_20191108.zip -> FY(All)-012_Contracts_Delta_20191108_%.csv
        return strip_file_extension(download_job.file_name)

    file_name_pattern = VALUE_MAPPINGS[source.source_type]["download_name"]
    timestamp = datetime.strftime(datetime.now(timezone.utc), "%Y-%m-%d_H%HM%MS%S")

    if source.is_for_idv or source.is_for_contract:
        data_file_name = file_name_pattern.format(piid=slugify_text_for_file_names(piid, "UNKNOWN", 50))
    elif source.is_for_assistance:
        data_file_name = file_name_pattern.format(
            assistance_id=slugify_text_for_file_names(assistance_id, "UNKNOWN", 50)
        )
    elif source.source_type == "disaster_recipient":
        data_file_name = file_name_pattern.format(award_category=source.award_category, timestamp=timestamp)
    else:
        if source.agency_code == "all":
            agency = "All"
        else:
            agency = str(source.agency_code)

        request = json.loads(download_job.json_request)
        filters = request["filters"]
        if request.get("limit"):
            agency = ""
        elif source.file_type not in ("treasury_account", "federal_account"):
            agency = f"{agency}_"

        data_file_name = file_name_pattern.format(
            agency=agency,
            data_quarters=construct_data_date_range(filters),
            level=d_map[source.file_type],
            timestamp=timestamp,
            type=d_map[source.file_type],
        )

    return data_file_name


def parse_source(source, columns, download_job, working_dir, piid, assistance_id, zip_file_path, limit, file_format):
    """Write to delimited text file(s) and zip file(s) using the source data"""

    data_file_name = build_data_file_name(source, download_job, piid, assistance_id)

    source_query = source.row_emitter(columns)
    extension = FILE_FORMATS[file_format]["extension"]
    source.file_name = f"{data_file_name}.{extension}"
    source_path = os.path.join(working_dir, source.file_name)

    write_to_log(message=f"Preparing to download data as {source.file_name}", download_job=download_job)

    # Generate the query file; values, limits, dates fixed
    export_query = generate_export_query(source_query, limit, source, columns, file_format)
    temp_file, temp_file_path = generate_export_query_temp_file(export_query, download_job)

    start_time = time.perf_counter()
    try:
        # Create a separate process to run the PSQL command; wait
        psql_process = multiprocessing.Process(target=execute_psql, args=(temp_file_path, source_path, download_job))
        psql_process.start()
        wait_for_process(psql_process, start_time, download_job)

        delim = FILE_FORMATS[file_format]["delimiter"]

        # Log how many rows we have
        write_to_log(message="Counting rows in delimited text file", download_job=download_job)
        try:
            download_job.number_of_rows += count_rows_in_delimited_file(
                filename=source_path, has_header=True, delimiter=delim
            )
        except Exception:
            write_to_log(
                message="Unable to obtain delimited text file line count", is_error=True, download_job=download_job
            )
        download_job.save()

        # Create a separate process to split the large data files into smaller file and write to zip; wait
        zip_process = multiprocessing.Process(
            target=split_and_zip_data_files,
            args=(zip_file_path, source_path, data_file_name, file_format, download_job),
        )
        zip_process.start()
        wait_for_process(zip_process, start_time, download_job)
        download_job.save()
    except Exception as e:
        raise e
    finally:
        # Remove temporary files
        os.close(temp_file)
        os.remove(temp_file_path)


def split_and_zip_data_files(zip_file_path, source_path, data_file_name, file_format, download_job=None):
    try:
        # Split data files into separate files
        # e.g. `Assistance_prime_transactions_delta_%s.csv`
        log_time = time.perf_counter()
        delim = FILE_FORMATS[file_format]["delimiter"]
        extension = FILE_FORMATS[file_format]["extension"]

        output_template = f"{data_file_name}_%s.{extension}"
        write_to_log(message="Beginning the delimited text file partition", download_job=download_job)
        list_of_files = partition_large_delimited_file(
            file_path=source_path, delimiter=delim, row_limit=EXCEL_ROW_LIMIT, output_name_template=output_template
        )

        msg = f"Partitioning data into {len(list_of_files)} files took {time.perf_counter() - log_time:.4f}s"
        write_to_log(message=msg, download_job=download_job)

        # Zip the split files into one zipfile
        write_to_log(message="Beginning zipping and compression", download_job=download_job)
        log_time = time.perf_counter()
        append_files_to_zip_file(list_of_files, zip_file_path)

        write_to_log(
            message=f"Writing to zipfile took {time.perf_counter() - log_time:.4f}s", download_job=download_job
        )

    except Exception as e:
        message = "Exception while partitioning text file"
        if download_job:
            fail_download(download_job, e, message)
            write_to_log(message=message, download_job=download_job, is_error=True)
        logger.error(e)
        raise e


def start_download(download_job):
    # Update job attributes
    download_job.job_status_id = JOB_STATUS_DICT["running"]
    download_job.number_of_rows = 0
    download_job.number_of_columns = 0
    download_job.file_size = 0
    download_job.save()

    write_to_log(message=f"Starting to process DownloadJob {download_job.download_job_id}", download_job=download_job)

    return download_job.file_name


def finish_download(download_job):
    download_job.job_status_id = JOB_STATUS_DICT["finished"]
    download_job.save()

    write_to_log(message=f"Finished processing DownloadJob {download_job.download_job_id}", download_job=download_job)

    return download_job.file_name


def wait_for_process(process, start_time, download_job):
    """Wait for the process to complete, throw errors for timeouts or Process exceptions"""
    log_time = time.perf_counter()

    # Let the thread run until it finishes (max MAX_VISIBILITY_TIMEOUT), with a buffer of DOWNLOAD_VISIBILITY_TIMEOUT
    sleep_count = 0
    while process.is_alive():
        if (
            download_job
            and not download_job.monthly_download
            and (time.perf_counter() - start_time) > MAX_VISIBILITY_TIMEOUT
        ):
            break
        if sleep_count < 10:
            time.sleep(WAIT_FOR_PROCESS_SLEEP / 5)
        else:
            time.sleep(WAIT_FOR_PROCESS_SLEEP)
        sleep_count += 1

    over_time = (time.perf_counter() - start_time) >= MAX_VISIBILITY_TIMEOUT
    if download_job and (not download_job.monthly_download and over_time) or process.exitcode != 0:
        if process.is_alive():
            # Process is running for longer than MAX_VISIBILITY_TIMEOUT, kill it
            write_to_log(
                message=f"Attempting to terminate process (pid {process.pid})", download_job=download_job, is_error=True
            )
            process.terminate()
            e = TimeoutError(
                f"DownloadJob {download_job.download_job_id} lasted longer than {MAX_VISIBILITY_TIMEOUT / 3600} hours"
            )
        else:
            # An error occurred in the process
            e = Exception("Command failed. Please see the logs for details.")

        raise e

    return time.perf_counter() - log_time


def generate_export_query(source_query, limit, source, columns, file_format):
    if limit:
        source_query = source_query[:limit]
    query_annotated = apply_annotations_to_sql(generate_raw_quoted_query(source_query), source.columns(columns))
    options = FILE_FORMATS[file_format]["options"]
    return r"\COPY ({}) TO STDOUT {}".format(query_annotated, options)


def generate_export_query_temp_file(export_query, download_job, temp_dir=None):
    write_to_log(message=f"Saving PSQL Query: {export_query}", download_job=download_job, is_debug=True)
    dir_name = "/tmp"
    if temp_dir:
        dir_name = temp_dir
    # Create a unique temporary file to hold the raw query, using \copy
    (temp_sql_file, temp_sql_file_path) = tempfile.mkstemp(prefix="bd_sql_", dir=dir_name)

    with open(temp_sql_file_path, "w") as file:
        file.write(export_query)

    return temp_sql_file, temp_sql_file_path


def apply_annotations_to_sql(raw_query, aliases):
    """
    Django's ORM understandably doesn't allow aliases to be the same names as other fields available. However, if we
    want to use the efficiency of psql's COPY method and keep the column names, we need to allow these scenarios. This
    function simply outputs a modified raw sql which does the aliasing, allowing these scenarios.
    """

    cte_sql, select_statements = _select_columns(raw_query)

    DIRECT_SELECT_QUERY_REGEX = r'^[^ ]*\."[^"]*"$'  # Django is pretty consistent with how it prints out queries
    # Create a list from the non-derived values between SELECT and FROM
    selects_list = [str for str in select_statements if re.search(DIRECT_SELECT_QUERY_REGEX, str)]

    # Create a list from the derived values between SELECT and FROM
    aliased_list = [str for str in select_statements if not re.search(DIRECT_SELECT_QUERY_REGEX, str.strip())]
    deriv_dict = {}
    for str in aliased_list:
        split_string = _top_level_split(str, " AS ")
        alias = split_string[1].replace('"', "").replace(",", "").strip()
        if alias not in aliases:
            raise Exception(f'alias "{alias}" not found!')
        deriv_dict[alias] = split_string[0]

    # Match aliases with their values
    values_list = [
        f'{deriv_dict[alias] if alias in deriv_dict else selects_list.pop(0)} AS "{alias}"' for alias in aliases
    ]

    sql = raw_query.replace(_top_level_split(raw_query, "FROM")[0], "SELECT " + ", ".join(values_list), 1)

    if cte_sql:
        sql = f"{cte_sql} {sql}"

    # Now that we've converted the queryset to SQL, cleaned up aliasing for non-annotated fields, and sorted
    # the SELECT columns, there's one final step.  The Django ORM does now allow alias names to conflict with
    # column/field names on the underlying model.  For annotated fields, naming conflict exceptions occur at
    # the time they are applied to the queryset which means they never get to this function.  To work around
    # this, we give them a temporary name that cannot conflict with a field name on the model by appending
    # the suffix specified by NAMING_CONFLICT_DISCRIMINATOR.  Now that we have the "final" SQL, we must remove
    # that suffix.
    return sql.replace(NAMING_CONFLICT_DISCRIMINATOR, "")


def _select_columns(sql: str) -> Tuple[str, List[str]]:
    in_quotes = False
    in_cte = False
    parens_depth = 0
    last_processed_index = 0
    cte_sql = None
    retval = []

    for index, char in enumerate(sql):
        if char == '"':
            in_quotes = not in_quotes
        if in_quotes:
            continue
        if char == "(":
            parens_depth = parens_depth + 1
            if in_cte:
                continue
        if char == ")":
            parens_depth = parens_depth - 1
            if in_cte and parens_depth == 0:
                in_cte = False
                cte_sql = sql[: index + 1]
                last_processed_index = index

        if parens_depth == 0 and not in_cte:
            # Set flag to ignore the CTE
            if sql[index : index + 5] == "WITH ":
                in_cte = True
            # Ignore the SELECT statement
            if sql[index : index + 6] == "SELECT":
                last_processed_index = index + 6
            # If there is a FROM at the bottom level, we have all the values we need and can return
            if sql[index : index + 4] == "FROM":
                retval.append(sql[last_processed_index:index].strip())
                return cte_sql, retval
            # If there is a comma on the bottom level, add another select value and start parsing a new one
            if char == ",":
                retval.append(sql[last_processed_index:index].strip())
                last_processed_index = index + 1  # skips the comma by design

    return cte_sql, retval  # this will almost certainly error out later.


def _top_level_split(sql, splitter):
    in_quotes = False
    parens_depth = 0
    for index, char in enumerate(sql):
        if char == '"':
            in_quotes = not in_quotes
        if in_quotes:
            continue
        if char == "(":
            parens_depth = parens_depth + 1
        if char == ")":
            parens_depth = parens_depth - 1

        if parens_depth == 0:
            if sql[index : index + len(splitter)] == splitter:
                return [sql[:index], sql[index + len(splitter) :]]
    raise Exception(f"SQL string ${sql} cannot be split on ${splitter}")


def execute_psql(temp_sql_file_path, source_path, download_job):
    """Executes a single PSQL command within its own Subprocess"""
    try:
        log_time = time.perf_counter()
        temp_env = os.environ.copy()
        if not download_job.monthly_download:
            # Since terminating the process isn't guarenteed to end the DB statement, add timeout to client connection
            temp_env["PGOPTIONS"] = f"--statement-timeout={settings.DOWNLOAD_DB_TIMEOUT_IN_HOURS}h"

        cat_command = subprocess.Popen(["cat", temp_sql_file_path], stdout=subprocess.PIPE)
        subprocess.check_output(
            ["psql", "-q", "-o", source_path, retrieve_db_string(), "-v", "ON_ERROR_STOP=1"],
            stdin=cat_command.stdout,
            stderr=subprocess.STDOUT,
            env=temp_env,
        )

        duration = time.perf_counter() - log_time
        write_to_log(
            message=f"Wrote {os.path.basename(source_path)}, took {duration:.4f} seconds", download_job=download_job
        )
    except Exception as e:
        if not settings.IS_LOCAL:
            # Not logging the command as it can contain the database connection string
            e.cmd = "[redacted psql command]"
        logger.error(e)
        sql = subprocess.check_output(["cat", temp_sql_file_path]).decode()
        logger.error(f"Faulty SQL: {sql}")
        raise e


def retrieve_db_string():
    """It is necessary for this to be a function so the test suite can mock the connection string"""
    return settings.DOWNLOAD_DATABASE_URL


def strip_file_extension(file_name):
    return os.path.splitext(os.path.basename(file_name))[0]


def fail_download(download_job, exception, message):
    stack_trace = "".join(
        traceback.format_exception(etype=type(exception), value=exception, tb=exception.__traceback__)
    )
    download_job.error_message = f"{message}:\n{stack_trace}"
    download_job.job_status_id = JOB_STATUS_DICT["failed"]
    download_job.save()


def add_data_dictionary_to_zip(working_dir, zip_file_path):
    write_to_log(message="Adding data dictionary to zip file")
    data_dictionary_file_name = "Data_Dictionary_Crosswalk.xlsx"
    data_dictionary_file_path = os.path.join(working_dir, data_dictionary_file_name)
    data_dictionary_url = settings.DATA_DICTIONARY_DOWNLOAD_URL
    RetrieveFileFromUri(data_dictionary_url).copy(data_dictionary_file_path)
    append_files_to_zip_file([data_dictionary_file_path], zip_file_path)


def _kill_spawned_processes(download_job=None):
    """Cleanup (kill) any spawned child processes during this job run"""
    job = ps.Process(os.getpid())
    for spawn_of_job in job.children(recursive=True):
        write_to_log(
            message=f"Attempting to terminate child process with PID [{spawn_of_job.pid}] and name "
            f"[{spawn_of_job.name}]",
            download_job=download_job,
            is_error=True,
        )
        try:
            spawn_of_job.kill()
        except ps.NoSuchProcess:
            pass


def create_empty_data_file(
    source: DownloadSource,
    download_job: DownloadJob,
    working_dir: str,
    piid: str,
    assistance_id: str,
    zip_file_path: str,
    file_format: str,
) -> None:
    data_file_name = build_data_file_name(source, download_job, piid, assistance_id)
    extension = FILE_FORMATS[file_format]["extension"]
    source.file_name = f"{data_file_name}.{extension}"
    source_path = os.path.join(working_dir, source.file_name)
    write_to_log(
        message=f"Skipping download of {source.file_name} due to no valid columns provided", download_job=download_job
    )
    Path(source_path).touch()
    append_files_to_zip_file([source_path], zip_file_path)
