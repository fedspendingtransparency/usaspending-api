import json
import logging
import multiprocessing
import os
from copy import deepcopy
from pathlib import Path
from typing import Optional, Tuple, List
from datetime import datetime, timezone

import psutil as ps
import re
import shutil
import subprocess
import tempfile
import time
import traceback

from django.conf import settings
from django.db.models import QuerySet
from django.db.models.expressions import Ref

from opentelemetry import trace
from opentelemetry.trace import SpanKind

from usaspending_api.download.models.download_job_lookup import DownloadJobLookup
from usaspending_api.search.filters.time_period.decorators import NEW_AWARDS_ONLY_KEYWORD
from usaspending_api.settings import MAX_DOWNLOAD_LIMIT
from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping, assistance_type_mapping, idv_type_mapping
from usaspending_api.common.csv_helpers import count_rows_in_delimited_file, partition_large_delimited_file
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.orm_helpers import generate_raw_quoted_query
from usaspending_api.common.helpers.s3_helpers import multipart_upload
from usaspending_api.common.helpers.text_helpers import slugify_text_for_file_names
from usaspending_api.common.tracing import SubprocessTrace
from usaspending_api.download.download_utils import construct_data_date_range
from usaspending_api.download.filestreaming import NAMING_CONFLICT_DISCRIMINATOR
from usaspending_api.download.filestreaming.download_source import DownloadSource
from usaspending_api.download.filestreaming.file_description import build_file_description, save_file_description
from usaspending_api.download.filestreaming.zip_file import append_files_to_zip_file
from usaspending_api.download.helpers import verify_requested_columns_available, write_to_download_log as write_to_log
from usaspending_api.download.lookups import JOB_STATUS_DICT, VALUE_MAPPINGS, FILE_FORMATS
from usaspending_api.download.models.download_job import DownloadJob
from usaspending_api.common.helpers.s3_helpers import download_s3_object

DOWNLOAD_VISIBILITY_TIMEOUT = 60 * 10
MAX_VISIBILITY_TIMEOUT = 60 * 60 * settings.DOWNLOAD_DB_TIMEOUT_IN_HOURS
EXCEL_ROW_LIMIT = 1000000
WAIT_FOR_PROCESS_SLEEP = 5
JOB_TYPE = "USAspendingDownloader"

logger = logging.getLogger(__name__)

# Set up the OpenTelemetry tracer provider
tracer = trace.get_tracer_provider().get_tracer(__name__)


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
    request_type = json_request.get("request_type")

    file_name = start_download(download_job)

    with SubprocessTrace(
        name=f"job.{JOB_TYPE}.generate_download_{request_type}",
        kind=SpanKind.INTERNAL,
        service="bulk-download",
    ) as main_trace:
        main_trace.set_attributes(
            {
                "service": "bulk-download",
                "span_type": "Internal",
                "job_type": str(JOB_TYPE),
                "message": "Creating data archive files from the download job object",
                # download job details
                "download_job_id": str(download_job.download_job_id),
                "download_job_status": str(download_job.job_status.name),
                "download_file_name": str(download_job.file_name),
                "download_file_size": download_job.file_size if download_job.file_size is not None else 0,
                "number_of_rows": download_job.number_of_rows if download_job.number_of_rows is not None else 0,
                "number_of_columns": (
                    download_job.number_of_columns if download_job.number_of_columns is not None else 0
                ),
                "error_message": download_job.error_message if download_job.error_message else "",
                "monthly_download": str(download_job.monthly_download),
                "json_request": str(download_job.json_request) if download_job.json_request else "",
                "file_name": str(file_name),
            }
        )

    working_dir = None
    try:
        if limit is not None and limit > MAX_DOWNLOAD_LIMIT:

            with SubprocessTrace(
                name=f"job.{JOB_TYPE}.generate_download_{request_type}",
                kind=SpanKind.INTERNAL,
                service="bulk-download",
            ) as limit_exceeded:
                limit_exceeded.set_attributes(
                    {
                        "message": f"Unable to process this download because it includes more than the current limit of {MAX_DOWNLOAD_LIMIT} records",
                        "limit": limit,
                    }
                )

            raise Exception(
                f"Unable to process this download because it includes more than the current limit of {MAX_DOWNLOAD_LIMIT} records"
            )

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
        sources = get_download_sources(json_request, download_job, origination)
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
        with SubprocessTrace(
            name=f"job.{JOB_TYPE}.generate_download",
            kind=SpanKind.INTERNAL,
            service="bulk-download",
        ) as error_span:
            error_span.set_attributes(
                {
                    "service": "bulk-download",
                    "span_type": "Internal",
                    "message": exc_msg,
                    "error": str(e),
                }
            )
            fail_download(download_job, e, exc_msg)
            raise InvalidParameterException(e)
    except Exception as e:
        # Set error message; job_status_id will be set in download_sqs_worker.handle()
        exc_msg = "An exception was raised while attempting to process the DownloadJob"
        with SubprocessTrace(
            name=f"job.{JOB_TYPE}.process_download_job",
            kind=SpanKind.INTERNAL,
            service="bulk-download",
        ) as error_span:
            error_span.set_attributes(
                {
                    "service": "bulk-download",
                    "span_type": "Internal",
                    "message": exc_msg,
                    "error": str(e),
                }
            )
            fail_download(download_job, e, exc_msg)
            raise Exception(download_job.error_message) from e
    finally:
        # Remove working directory
        if working_dir and os.path.exists(working_dir):
            shutil.rmtree(working_dir)
        _kill_spawned_processes(download_job)
        DownloadJobLookup.objects.filter(download_job_id=download_job.download_job_id).delete()

    # push file to S3 bucket, if not local
    if not settings.IS_LOCAL:
        with SubprocessTrace(
            name=f"job.{JOB_TYPE}.download.s3",
            kind=SpanKind.INTERNAL,
            service="bulk-download",
        ) as span:
            span.set_attributes(
                {
                    "service": "bulk-download",
                    "span_type": "Internal",
                    "resource": f"s3://{settings.BULK_DOWNLOAD_S3_BUCKET_NAME}",
                    "message": "Push file to S3 bucket, if not local",
                }
            )

        with SubprocessTrace(
            name=f"job.{JOB_TYPE}.s3.command",
            kind=SpanKind.SERVER,
            service="bulk-download",
        ) as s3_span:
            s3_span.set_attributes(
                {
                    "service": "aws.s3",
                    "span_type": "WEB",
                    "resource": ".".join(
                        [multipart_upload.__module__, (multipart_upload.__qualname__ or multipart_upload.__name__)]
                    ),
                }
            )

            # NOTE: Traces still not auto-picking-up aws.s3 service upload activity
            # Could be that the patches for boto and botocore don't cover the newer boto3 S3Transfer upload approach
            try:
                bucket = settings.BULK_DOWNLOAD_S3_BUCKET_NAME
                region = settings.USASPENDING_AWS_REGION
                s3_span.set_attributes({"bucket": bucket, "region": region, "file": zip_file_path})
                start_uploading = time.perf_counter()
                multipart_upload(bucket, region, zip_file_path, os.path.basename(zip_file_path))
                write_to_log(
                    message=f"Uploading took {time.perf_counter() - start_uploading:.2f}s", download_job=download_job
                )
            except Exception as e:
                exc_msg = "An exception was raised while attempting to upload the file"
                with SubprocessTrace(
                    name=f"job.{JOB_TYPE}.upload_file_to_aws",
                    kind=SpanKind.SERVER,
                    service="bulk-download",
                ) as error_span:
                    error_span.set_attributes(
                        {
                            "service": "bulk-download",
                            "span_type": "Internal",
                            "message": exc_msg,
                            "error": str(e),
                        }
                    )
                    # Set error message; job_status_id will be set in download_sqs_worker.handle()
                    fail_download(download_job, e, exc_msg)
                    if isinstance(e, InvalidParameterException):
                        raise InvalidParameterException(e)
                    else:
                        raise Exception(download_job.error_message) from e
            finally:
                # Remove generated file
                if os.path.exists(zip_file_path):
                    os.remove(zip_file_path)
                _kill_spawned_processes(download_job)

    return finish_download(download_job)


def get_download_sources(
    json_request: dict, download_job: DownloadJob = None, origination: Optional[str] = None
) -> List[DownloadSource]:
    download_sources = []
    for download_type in json_request["download_types"]:
        agency_id = json_request.get("agency", "all")
        filter_function = VALUE_MAPPINGS[download_type]["filter_function"]
        download_type_table = VALUE_MAPPINGS[download_type]["table"]

        if VALUE_MAPPINGS[download_type]["source_type"] == "award":
            # Award downloads

            # Use correct date range columns for advanced search
            # (Will not change anything for keyword search since "time_period" is not provided)
            filters = deepcopy(json_request["filters"])

            # sub awards do not support `new_awards_only` date type
            #   the reason we need this check is because downloads are
            #   sometimes a mix between prime awards and subawards
            #   and share the same filters. In the cases where the
            #   download requests `new_awards_only` we only want to apply this
            #   date type to the prime award summaries
            if json_request["filters"].get("time_period") is not None and (
                download_type == "sub_awards" or download_type == "elasticsearch_sub_awards"
            ):
                for time_period in filters["time_period"]:
                    if time_period.get("date_type") == NEW_AWARDS_ONLY_KEYWORD:
                        del time_period["date_type"]
                    if time_period.get("date_type") == "date_signed":
                        time_period["date_type"] = "action_date"
            if (
                download_type == "elasticsearch_awards"
                or download_type == "elasticsearch_transactions"
                or download_type == "elasticsearch_sub_awards"
            ):
                queryset = filter_function(filters, download_job=download_job)
            else:
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
                d1_source = DownloadSource(
                    VALUE_MAPPINGS[download_type]["table_name"], "d1", download_type, agency_id, filters
                )
                d1_filters = {f"{VALUE_MAPPINGS[download_type].get('is_fpds_join', '')}is_fpds": True}
                d1_source.queryset = queryset & download_type_table.objects.filter(**d1_filters)
                download_sources.append(d1_source)

            if award_type_codes & set(assistance_type_mapping.keys()) or ("grant" in award_type_codes):
                # only generate d2 files if the user is asking for assistance data
                d2_source = DownloadSource(
                    VALUE_MAPPINGS[download_type]["table_name"], "d2", download_type, agency_id, filters
                )
                d2_filters = {f"{VALUE_MAPPINGS[download_type].get('is_fpds_join', '')}is_fpds": False}
                d2_source.queryset = queryset & download_type_table.objects.filter(**d2_filters)
                download_sources.append(d2_source)

        elif VALUE_MAPPINGS[download_type]["source_type"] == "account":
            # Account downloads
            filters = {**json_request["filters"], **json_request.get("account_filters", {})}

            if "is_fpds_join" in VALUE_MAPPINGS[download_type]:
                # Contracts
                d1_account_source = DownloadSource(
                    VALUE_MAPPINGS[download_type]["table_name"],
                    json_request["account_level"],
                    download_type,
                    agency_id,
                    extra_file_type="Contracts_",
                )
                d1_filters = {**filters, f"{VALUE_MAPPINGS[download_type].get('is_fpds_join', '')}is_fpds": True}
                d1_account_source.queryset = filter_function(
                    download_type,
                    VALUE_MAPPINGS[download_type]["table"],
                    d1_filters,
                    json_request["account_level"],
                )
                download_sources.append(d1_account_source)

                # Assistance
                d2_account_source = DownloadSource(
                    VALUE_MAPPINGS[download_type]["table_name"],
                    json_request["account_level"],
                    download_type,
                    agency_id,
                    extra_file_type="Assistance_",
                )
                d2_filters = {**filters, f"{VALUE_MAPPINGS[download_type].get('is_fpds_join', '')}is_fpds": False}
                d2_account_source.queryset = filter_function(
                    download_type,
                    VALUE_MAPPINGS[download_type]["table"],
                    d2_filters,
                    json_request["account_level"],
                )
                download_sources.append(d2_account_source)

                # Unlinked
                unlinked_account_source = DownloadSource(
                    VALUE_MAPPINGS[download_type]["table_name"],
                    json_request["account_level"],
                    download_type,
                    agency_id,
                    extra_file_type="Unlinked_",
                )
                unlinked_filters = {**filters, "unlinked": True}
                unlinked_account_source.queryset = filter_function(
                    download_type,
                    VALUE_MAPPINGS[download_type]["table"],
                    unlinked_filters,
                    json_request["account_level"],
                )
                download_sources.append(unlinked_account_source)

            else:
                account_source = DownloadSource(
                    VALUE_MAPPINGS[download_type]["table_name"], json_request["account_level"], download_type, agency_id
                )
                account_source.queryset = filter_function(
                    download_type,
                    VALUE_MAPPINGS[download_type]["table"],
                    filters,
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
    if download_job and download_job.monthly_download:
        # For monthly archives, use the existing detailed zip filename for the data files
        # e.g. FY(All)-012_Contracts_Delta_20191108.zip -> FY(All)-012_Contracts_Delta_20191108_%.csv
        return strip_file_extension(download_job.file_name)

    file_name_pattern = VALUE_MAPPINGS[source.source_type]["download_name"]
    timestamp = datetime.strftime(datetime.now(timezone.utc), "%Y-%m-%d_H%HM%MS%S")

    if source.is_for_idv or source.is_for_contract:
        file_name_values = {"piid": slugify_text_for_file_names(piid, "UNKNOWN", 50)}
    elif source.is_for_assistance:
        file_name_values = {"assistance_id": slugify_text_for_file_names(assistance_id, "UNKNOWN", 50)}
    elif source.source_type == "disaster_recipient":
        file_name_values = {"award_category": source.award_category, "timestamp": timestamp}
    else:
        d_map = {"d1": "Contracts", "d2": "Assistance", "treasury_account": "TAS", "federal_account": "FA"}

        if source.agency_code == "all":
            agency = "All"
        else:
            agency = str(source.agency_code)

        request = json.loads(download_job.json_request)
        filters = request["filters"]

        if request.get("limit") or (
            request.get("request_type") == "disaster" and source.source_type in ("elasticsearch_awards", "sub_awards")
        ):
            agency = ""
        elif source.file_type not in ("treasury_account", "federal_account"):
            agency = f"{agency}_"

        if request.get("request_type") == "disaster":
            account_filters = request["account_filters"]
            current_fiscal_period = (
                f"FY{account_filters['latest_fiscal_year']}P{str(account_filters['latest_fiscal_period']).zfill(2)}"
            )
            data_quarters = f"{current_fiscal_period}-Present"
        else:
            data_quarters = construct_data_date_range(filters)

        file_name_values = {
            "agency": agency,
            "data_quarters": data_quarters,
            "level": d_map[source.file_type],
            "timestamp": timestamp,
            "type": d_map[source.file_type],
            "extra_file_type": source.extra_file_type,
        }

    return file_name_pattern.format(**file_name_values)


def parse_source(
    source: DownloadSource,
    columns: Optional[List[str]],
    download_job: DownloadJob,
    working_dir: str,
    piid: str,
    assistance_id: str,
    zip_file_path: str,
    limit: int,
    file_format: str,
):
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
        write_to_log(message=f"Running {source.file_name} using psql", download_job=download_job)
        psql_process.start()
        wait_for_process(psql_process, start_time, download_job)

        delim = FILE_FORMATS[file_format]["delimiter"]

        # Log how many rows we have
        write_to_log(message="Counting rows in delimited text file", download_job=download_job)
        try:
            number_of_rows = count_rows_in_delimited_file(filename=source_path, has_header=True, delimiter=delim)
            download_job.number_of_rows += number_of_rows
            write_to_log(message=f"Number of rows in text file: {number_of_rows}", download_job=download_job)
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
    with SubprocessTrace(
        name=f"job.{JOB_TYPE}.download.zip",
        kind=SpanKind.INTERNAL,
        service="bulk-download",
    ) as span:
        span.set_attributes(
            {
                "service": "bulk-download",
                "span_type": "Internal",
                "data_file_name": data_file_name,
                "file_format": file_format,
                "source_path": source_path,
                "zip_file_path": zip_file_path,
            }
        )

    try:
        # Split data files into separate files
        # e.g. `Assistance_prime_transactions_delta_%s.csv`
        log_time = time.perf_counter()
        delim = FILE_FORMATS[file_format]["delimiter"]
        extension = FILE_FORMATS[file_format]["extension"]

        output_template = f"{data_file_name}_%s.{extension}"
        write_to_log(message="Beginning the delimited text file partition", download_job=download_job)
        list_of_files = partition_large_delimited_file(
            download_job=download_job,
            file_path=source_path,
            delimiter=delim,
            row_limit=EXCEL_ROW_LIMIT,
            output_name_template=output_template,
        )
        span.set_attribute("file_parts", len(list_of_files))

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
        span.set_attribute("raised_exception", message)
        if download_job:
            fail_download(download_job, e, message)
        write_to_log(message=message, download_job=download_job, is_error=True)
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


def generate_export_query(
    source_query: QuerySet, limit: int, source: DownloadSource, column_subset: Optional[List[str]], file_format: str
) -> str:
    if limit:
        source_query = source_query[:limit]
    selected_columns = source.columns(column_subset)

    # Changes to how GROUP BY is handled by Django in the upgrade to 4.2 require that we capture the annotated columns
    # that will appear in the GROUP BY of the generated SQL. On the QuerySet the "group_by" property can be any of the
    # following: None, tuple of Refs / strings, or True (meaning all selected fields). In this case we expect either
    # None or a tuple, but want to protect against the possibility of "True".
    annotated_group_by_columns = []
    if isinstance(source_query.query.group_by, tuple):
        annotation_select = source_query.query.annotation_select
        annotation_select_reverse = {val: key for key, val in annotation_select.items()}
        for val in source_query.query.group_by:
            if isinstance(val, Ref) and annotation_select[val.refs] == val.source:
                annotated_group_by_columns.append(val.refs)
            elif val in annotation_select.values():
                annotated_group_by_columns.append(annotation_select_reverse[val])

    query_annotated = apply_annotations_to_sql(
        generate_raw_quoted_query(source_query), selected_columns, annotated_group_by_columns
    )
    options = FILE_FORMATS[file_format]["options"]
    return rf"\COPY ({query_annotated}) TO STDOUT {options}"


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


def apply_annotations_to_sql(
    raw_query: str, aliases: List[str], annotated_group_by_columns: Optional[List[str]] = None
):
    """
    Django's ORM understandably doesn't allow aliases to be the same names as other fields available. However, if we
    want to use the efficiency of psql's COPY method and keep the column names, we need to allow these scenarios. This
    function simply outputs a modified raw sql which does the aliasing, allowing these scenarios.
    """

    cte_sql, select_statements = _select_columns(raw_query)

    DIRECT_SELECT_QUERY_REGEX = r'^[^ ]*\."[^"]*"$'  # Django is pretty consistent with how it prints out queries
    # Create a list from the non-derived values between SELECT and FROM
    selects_list = [val for val in select_statements if re.search(DIRECT_SELECT_QUERY_REGEX, val)]

    # Create a list from the derived values between SELECT and FROM
    aliased_list = [
        (idx + 1, val)
        for idx, val in enumerate(select_statements)
        if not re.search(DIRECT_SELECT_QUERY_REGEX, val.strip())
    ]
    deriv_dict = {}

    # In the upgrade to Django 4.2 it was found that the change from psycopg2 to 3 by Django resulted in some
    # breaking changes for they way we manage our downloads. In this particular change the GROUP BY was changed to
    # now reference annotated fields by their column position. The download logic handles repositioning columns
    # into the order that is expected and thus breaks the use of positional reference by the GROUP BY. In order to
    # continue processing downloads in a similar way, the logic below was updated to replace the positional value with
    # the annotated value found in the column, allowing the SQL query to work as expected regardless of column order.
    group_by_to_replace = []

    for idx, val in aliased_list:
        split_string = _top_level_split(val, " AS ")
        alias = split_string[1].replace('"', "").replace(",", "").strip()
        if alias not in aliases:
            raise Exception(f'alias "{alias}" not found!')
        col_select = split_string[0]
        deriv_dict[alias] = col_select
        if annotated_group_by_columns and alias in annotated_group_by_columns:
            group_by_to_replace.append((idx, col_select))

    if group_by_to_replace:
        first_half_query, second_half_query = _top_level_split(raw_query, " GROUP BY ")
        # Fist we replace the position with a valid parameter we can use for formatting
        for idx, _ in group_by_to_replace:
            # It is assumed that all non-positional values in the GROUP BY are column names meaning the first number
            # matching the value of "idx" will be the position. However, this is not guaranteed in the rest of the
            # query, so we make sure to stop after the first match found.
            second_half_query = second_half_query.replace(f" {idx}", f" {{idx_{idx}}}", 1)
        second_half_query = second_half_query.format(
            **{f"idx_{idx}": col_select for idx, col_select in group_by_to_replace}
        )
        raw_query = f"{first_half_query} GROUP BY {second_half_query}"

    # Match aliases with their values
    values_list = [
        f'{deriv_dict[alias] if alias in deriv_dict else selects_list.pop(0)} AS "{alias}"' for alias in aliases
    ]

    sql = raw_query.replace(_top_level_split(raw_query, "FROM")[0], f"SELECT {', '.join(values_list)} ", 1)

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
    download_sql = Path(temp_sql_file_path).read_text()
    if download_sql.startswith("\\COPY"):
        # Trace library parses the SQL, but cannot understand the psql-specific \COPY command. Use standard COPY here.
        download_sql = download_sql[1:]

    # Stack 3 context managers: (1) psql code, (2) Download replica query, (3) (same) Postgres query
    subprocess_trace = SubprocessTrace(
        name=f"job.{JOB_TYPE}.download.psql",
        kind=SpanKind.INTERNAL,
        service="bulk-download",
    )

    with subprocess_trace as span:
        span.set_attributes(
            {
                "service": "bulk-download",
                "resource": str(download_sql),
                "span_type": "Internal",
                "source_path": str(source_path),
                # download job details
                "download_job_id": str(download_job.download_job_id),
                "download_job_status": str(download_job.job_status.name),
                "download_file_name": str(download_job.file_name),
                "download_file_size": download_job.file_size if download_job.file_size is not None else 0,
                "number_of_rows": download_job.number_of_rows if download_job.number_of_rows is not None else 0,
                "number_of_columns": (
                    download_job.number_of_columns if download_job.number_of_columns is not None else 0
                ),
                "error_message": download_job.error_message if download_job.error_message else "",
                "monthly_download": str(download_job.monthly_download),
                "json_request": str(download_job.json_request) if download_job.json_request else "",
            }
        )

        try:
            log_time = time.perf_counter()
            temp_env = os.environ.copy()
            if download_job and not download_job.monthly_download:
                # Since terminating the process isn't guaranteed to end the DB statement, add timeout to client connection
                temp_env["PGOPTIONS"] = (
                    f"--statement-timeout={settings.DOWNLOAD_DB_TIMEOUT_IN_HOURS}h "
                    f"--work-mem={settings.DOWNLOAD_DB_WORK_MEM_IN_MB}MB"
                )

            cat_command = subprocess.Popen(["cat", temp_sql_file_path], stdout=subprocess.PIPE)
            subprocess.check_output(
                ["psql", "-q", "-o", source_path, retrieve_db_string(), "-v", "ON_ERROR_STOP=1"],
                stdin=cat_command.stdout,
                stderr=subprocess.STDOUT,
                env=temp_env,
            )

            duration = time.perf_counter() - log_time
            write_to_log(
                message=f"Wrote {os.path.basename(source_path)}, took {duration:.4f} seconds",
                download_job=download_job,
            )
        except subprocess.CalledProcessError as e:
            write_to_log(message=f"PSQL Error: {e.output.decode()}", is_error=True, download_job=download_job)
            raise e
        except Exception as e:
            if not settings.IS_LOCAL:
                # Not logging the command as it can contain the database connection string
                e.cmd = "[redacted psql command]"
            write_to_log(message=e, is_error=True, download_job=download_job)
            sql = subprocess.check_output(["cat", temp_sql_file_path]).decode()
            write_to_log(message=f"Faulty SQL: {sql}", is_error=True, download_job=download_job)
            raise e


def retrieve_db_string():
    """It is necessary for this to be a function so the test suite can mock the connection string"""
    return settings.DOWNLOAD_DATABASE_URL


def strip_file_extension(file_name):
    return os.path.splitext(os.path.basename(file_name))[0]


def fail_download(download_job, exception, message):
    write_to_log(message=message, is_error=True, download_job=download_job)
    stack_trace = "".join(traceback.format_exception(type(exception), value=exception, tb=exception.__traceback__))
    download_job.error_message = f"{message}:\n{stack_trace}"
    download_job.job_status_id = JOB_STATUS_DICT["failed"]
    download_job.save()


def add_data_dictionary_to_zip(working_dir, zip_file_path):
    write_to_log(message="Adding data dictionary to zip file")
    data_dictionary_file_name = "Data_Dictionary_Crosswalk.xlsx"
    data_dictionary_file_path = os.path.join(working_dir, data_dictionary_file_name)

    logger.info(
        f"Retrieving the data dictionary from S3. Bucket: {settings.DATA_DICTIONARY_S3_BUCKET_NAME} Key: {settings.DATA_DICTIONARY_S3_KEY}"
    )
    logger.info(f"Saving the data dictionary to: {data_dictionary_file_path}")
    download_s3_object(
        bucket_name=settings.DATA_DICTIONARY_S3_BUCKET_NAME,
        key=settings.DATA_DICTIONARY_S3_KEY,
        file_path=data_dictionary_file_path,
        retry_count=settings.DATA_DICTIONARY_DOWNLOAD_RETRY_COUNT,
    )

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
