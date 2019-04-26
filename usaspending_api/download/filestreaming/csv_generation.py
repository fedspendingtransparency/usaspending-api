import json
import logging
import multiprocessing
import os
import re
import shutil
import subprocess
import tempfile
import time
import traceback

from django.conf import settings

from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping, assistance_type_mapping, idv_type_mapping
from usaspending_api.common.csv_helpers import count_rows_in_csv_file, partition_large_csv_file
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.sql_helpers import generate_raw_quoted_query
from usaspending_api.common.helpers.text_helpers import slugify_text_for_file_names
from usaspending_api.download.filestreaming.csv_source import CsvSource
from usaspending_api.download.filestreaming.file_description import build_file_description, save_file_description
from usaspending_api.download.filestreaming.zip_file import append_files_to_zip_file
from usaspending_api.download.helpers import (verify_requested_columns_available, multipart_upload,
                                              write_to_download_log as write_to_log)
from usaspending_api.download.lookups import JOB_STATUS_DICT, VALUE_MAPPINGS

DOWNLOAD_VISIBILITY_TIMEOUT = 60 * 10
MAX_VISIBILITY_TIMEOUT = 60 * 60 * 4
EXCEL_ROW_LIMIT = 1000000
WAIT_FOR_PROCESS_SLEEP = 5

logger = logging.getLogger('console')


def generate_csvs(download_job):
    """Derive the relevant file location and write CSVs to it"""

    # Parse data from download_job
    json_request = json.loads(download_job.json_request)
    columns = json_request.get('columns', None)
    limit = json_request.get('limit', None)
    piid = json_request.get('piid', None)

    file_name = start_download(download_job)
    try:
        # Create temporary files and working directory
        zip_file_path = settings.CSV_LOCAL_PATH + file_name
        working_dir = os.path.splitext(zip_file_path)[0]
        if not os.path.exists(working_dir):
            os.mkdir(working_dir)

        write_to_log(message='Generating {}'.format(file_name), download_job=download_job)

        # Generate sources from the JSON request object
        sources = get_csv_sources(json_request)
        for source in sources:
            # Parse and write data to the file
            download_job.number_of_columns = max(download_job.number_of_columns, len(source.columns(columns)))
            parse_source(source, columns, download_job, working_dir, piid, zip_file_path, limit)
        include_file_description = json_request.get('include_file_description')
        if include_file_description:
            write_to_log(message="Adding file description to zip file")
            file_description = build_file_description(include_file_description["source"], sources)
            file_description_path = save_file_description(
                working_dir, include_file_description["destination"], file_description)
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
        if os.path.exists(working_dir):
            shutil.rmtree(working_dir)

    try:
        # push file to S3 bucket, if not local
        if not settings.IS_LOCAL:
            bucket = settings.BULK_DOWNLOAD_S3_BUCKET_NAME
            region = settings.USASPENDING_AWS_REGION
            start_uploading = time.perf_counter()
            multipart_upload(bucket, region, zip_file_path, os.path.basename(zip_file_path))
            write_to_log(message='Uploading took {} seconds'.format(time.perf_counter() - start_uploading),
                         download_job=download_job)
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

    return finish_download(download_job)


def get_csv_sources(json_request):
    csv_sources = []
    for download_type in json_request['download_types']:
        agency_id = json_request['filters'].get('agency', 'all')
        filter_function = VALUE_MAPPINGS[download_type]['filter_function']
        download_type_table = VALUE_MAPPINGS[download_type]['table']

        if VALUE_MAPPINGS[download_type]['source_type'] == 'award':
            # Award downloads
            queryset = filter_function(json_request['filters'])
            award_type_codes = set(json_request['filters']['award_type_codes'])

            if award_type_codes & (set(contract_type_mapping.keys()) | set(idv_type_mapping.keys())):
                # only generate d1 files if the user is asking for contract data
                d1_source = CsvSource(VALUE_MAPPINGS[download_type]['table_name'], 'd1', download_type, agency_id)
                d1_filters = {'{}__isnull'.format(VALUE_MAPPINGS[download_type]['contract_data']): False}
                d1_source.queryset = queryset & download_type_table.objects.filter(**d1_filters)
                csv_sources.append(d1_source)

            if award_type_codes & set(assistance_type_mapping.keys()):
                # only generate d2 files if the user is asking for assistance data
                d2_source = CsvSource(VALUE_MAPPINGS[download_type]['table_name'], 'd2', download_type, agency_id)
                d2_filters = {'{}__isnull'.format(VALUE_MAPPINGS[download_type]['assistance_data']): False}
                d2_source.queryset = queryset & download_type_table.objects.filter(**d2_filters)
                csv_sources.append(d2_source)

            verify_requested_columns_available(tuple(csv_sources), json_request.get('columns', []))
        elif VALUE_MAPPINGS[download_type]['source_type'] == 'account':
            # Account downloads
            account_source = CsvSource(VALUE_MAPPINGS[download_type]['table_name'], json_request['account_level'],
                                       download_type, agency_id)
            account_source.queryset = filter_function(download_type, VALUE_MAPPINGS[download_type]['table'],
                                                      json_request['filters'], json_request['account_level'])
            csv_sources.append(account_source)

    return csv_sources


def parse_source(source, columns, download_job, working_dir, piid, zip_file_path, limit):
    """Write to csv and zip files using the source data"""
    d_map = {'d1': 'contracts', 'd2': 'assistance', 'treasury_account': 'treasury_account',
             'federal_account': 'federal_account'}
    if download_job and download_job.monthly_download:
        # Use existing detailed filename from parent file for monthly files
        # e.g. `019_Assistance_Delta_20180917_%s.csv`
        source_name = strip_file_extension(download_job.file_name)
    elif source.is_for_idv:
        file_name_pattern = VALUE_MAPPINGS[source.source_type]['download_name']
        source_name = file_name_pattern.format(piid=slugify_text_for_file_names(piid, "UNKNOWN", 50))
    else:
        source_name = '{}_{}_{}'.format(
            source.agency_code, d_map[source.file_type], VALUE_MAPPINGS[source.source_type]['download_name'])
    source_query = source.row_emitter(columns)
    source.file_name = '{}.csv'.format(source_name)
    source_path = os.path.join(working_dir, source.file_name)

    write_to_log(message='Preparing to download data as {}'.format(source_name), download_job=download_job)

    # Generate the query file; values, limits, dates fixed
    temp_file, temp_file_path = generate_temp_query_file(source_query, limit, source, download_job, columns)

    start_time = time.perf_counter()
    try:
        # Create a separate process to run the PSQL command; wait
        psql_process = multiprocessing.Process(target=execute_psql, args=(temp_file_path, source_path, download_job,))
        psql_process.start()
        wait_for_process(psql_process, start_time, download_job)

        # Log how many rows we have
        write_to_log(message='Counting rows in CSV', download_job=download_job)
        try:
            download_job.number_of_rows += count_rows_in_csv_file(filename=source_path, has_header=True)
        except Exception:
            write_to_log(message="Unable to obtain CSV line count", is_error=True, download_job=download_job)
        download_job.save()

        # Create a separate process to split the large csv into smaller csvs and write to zip; wait
        zip_process = multiprocessing.Process(target=split_and_zip_csvs, args=(zip_file_path, source_path, source_name,
                                                                               download_job))
        zip_process.start()
        wait_for_process(zip_process, start_time, download_job)
        download_job.save()
    except Exception as e:
        raise e
    finally:
        # Remove temporary files
        os.close(temp_file)
        os.remove(temp_file_path)


def split_and_zip_csvs(zip_file_path, source_path, source_name, download_job=None):
    try:
        # Split CSV into separate files
        # e.g. `Assistance_prime_transactions_delta_%s.csv`
        log_time = time.perf_counter()

        output_template = '{}_%s.csv'.format(source_name)
        write_to_log(message='Beginning the CSV file partition', download_job=download_job)
        list_of_csv_files = partition_large_csv_file(source_path, row_limit=EXCEL_ROW_LIMIT,
                                                     output_name_template=output_template)

        if download_job:
            write_to_log(
                message='Partitioning CSV file into {} files took {:.4f} seconds'.format(
                    len(list_of_csv_files),
                    time.perf_counter() - log_time
                ),
                download_job=download_job
            )

        # Zip the split CSVs into one zipfile
        write_to_log(message="Beginning zipping and compression", download_job=download_job)
        log_time = time.perf_counter()
        append_files_to_zip_file(list_of_csv_files, zip_file_path)

        if download_job:
            write_to_log(message='Writing to zipfile took {:.4f} seconds'.format(time.perf_counter() - log_time),
                         download_job=download_job)

    except Exception as e:
        message = "Exception while partitioning CSV"
        fail_download(download_job, e, message)
        write_to_log(message=message, download_job=download_job, is_error=True)
        logger.error(e)
        raise e


def start_download(download_job):
    # Update job attributes
    download_job.job_status_id = JOB_STATUS_DICT['running']
    download_job.number_of_rows = 0
    download_job.number_of_columns = 0
    download_job.file_size = 0
    download_job.save()

    write_to_log(message='Starting to process DownloadJob {}'.format(download_job.download_job_id),
                 download_job=download_job)

    return download_job.file_name


def finish_download(download_job):
    download_job.job_status_id = JOB_STATUS_DICT['finished']
    download_job.save()

    write_to_log(message='Finished processing DownloadJob {}'.format(download_job.download_job_id),
                 download_job=download_job)

    return download_job.file_name


def wait_for_process(process, start_time, download_job):
    """Wait for the process to complete, throw errors for timeouts or Process exceptions"""
    log_time = time.perf_counter()

    # Let the thread run until it finishes (max MAX_VISIBILITY_TIMEOUT), with a buffer of DOWNLOAD_VISIBILITY_TIMEOUT
    sleep_count = 0
    while process.is_alive() and (time.perf_counter() - start_time) < MAX_VISIBILITY_TIMEOUT:
        if sleep_count < 10:
            time.sleep(WAIT_FOR_PROCESS_SLEEP / 5)
        else:
            time.sleep(WAIT_FOR_PROCESS_SLEEP)
        sleep_count += 1

    if (time.perf_counter() - start_time) >= MAX_VISIBILITY_TIMEOUT or process.exitcode != 0:
        if process.is_alive():
            # Process is running for longer than MAX_VISIBILITY_TIMEOUT, kill it
            write_to_log(message='Attempting to terminate process (pid {})'.format(process.pid),
                         download_job=download_job, is_error=True)
            process.terminate()
            e = TimeoutError('DownloadJob {} lasted longer than {} hours'.format(download_job.download_job_id,
                                                                                 str(MAX_VISIBILITY_TIMEOUT / 3600)))
        else:
            # An error occurred in the process
            e = Exception('Command failed. Please see the logs for details.')

        raise e

    return time.perf_counter() - log_time


def generate_temp_query_file(source_query, limit, source, download_job, columns):
    if limit:
        source_query = source_query[:limit]
    csv_query_annotated = apply_annotations_to_sql(generate_raw_quoted_query(source_query), source.columns(columns))

    write_to_log(
        message='Creating PSQL Query: {}'.format(csv_query_annotated),
        download_job=download_job,
        is_debug=True
    )

    # Create a unique temporary file to hold the raw query, using \copy
    (temp_sql_file, temp_sql_file_path) = tempfile.mkstemp(prefix='bd_sql_', dir='/tmp')
    with open(temp_sql_file_path, "w") as file:
        file.write(r"\copy ({}) To STDOUT with CSV HEADER".format(csv_query_annotated))

    return temp_sql_file, temp_sql_file_path


def apply_annotations_to_sql(raw_query, aliases):
    """
    Django's ORM understandably doesn't allow aliases to be the same names as other fields available. However, if we
    want to use the efficiency of psql's COPY method and keep the column names, we need to allow these scenarios. This
    function simply outputs a modified raw sql which does the aliasing, allowing these scenarios.
    """
    aliases_copy = list(aliases)

    # Extract everything between the first SELECT and the last FROM
    query_before_group_by = raw_query.split('GROUP BY ')[0]
    query_before_from = re.sub("SELECT ", "", ' FROM'.join(re.split(' FROM', query_before_group_by)[:-1]), count=1)

    # Create a list from the non-derived values between SELECT and FROM
    selects_str = re.findall(r"SELECT (.*?) (CASE|CONCAT|SUM|COALESCE|\(SELECT|FROM)", raw_query)[0]
    just_selects = selects_str[0] if selects_str[1] == 'FROM' else selects_str[0][:-1]
    selects_list = [select.strip() for select in just_selects.strip().split(',')]

    # Create a list from the derived values between SELECT and FROM
    remove_selects = query_before_from.replace(selects_str[0], "")
    deriv_str_lookup = re.findall(r"(CASE|CONCAT|SUM|COALESCE|\(SELECT|)(.*?) AS (.*?)( |$)", remove_selects)
    deriv_dict = {}
    for str_match in deriv_str_lookup:
        # Remove trailing comma and surrounding quotes from the alias, add to dict, remove from alias list
        alias = str_match[2][:-1].strip() if str_match[2][-1:] == ',' else str_match[2].strip()
        if (alias[-1:] == "\"" and alias[:1] == "\"") or (alias[-1:] == "'" and alias[:1] == "'"):
            alias = alias[1:-1]
        deriv_dict[alias] = '{}{}'.format(str_match[0], str_match[1]).strip()
        aliases_copy.remove(alias)

    # Validate we have an alias for each value in the SELECT string
    if len(selects_list) != len(aliases_copy):
        raise Exception("Length of alises doesn't match the columns in selects")

    # Match aliases with their values
    values_list = ['{} AS \"{}\"'.format(deriv_dict[alias] if alias in deriv_dict else selects_list.pop(0), alias)
                   for alias in aliases]

    return raw_query.replace(query_before_from, ", ".join(values_list), 1)


def execute_psql(temp_sql_file_path, source_path, download_job):
    """Executes a single PSQL command within its own Subprocess"""
    try:
        log_time = time.perf_counter()

        cat_command = subprocess.Popen(['cat', temp_sql_file_path], stdout=subprocess.PIPE)
        subprocess.check_output(['psql', '-o', source_path, retrieve_db_string(), '-v', 'ON_ERROR_STOP=1'],
                                stdin=cat_command.stdout, stderr=subprocess.STDOUT)

        duration = time.perf_counter() - log_time
        write_to_log(
            message='Wrote {}, took {:.4f} seconds'.format(os.path.basename(source_path), duration),
            download_job=download_job)
    except subprocess.CalledProcessError as e:
        # Not logging the command as it can contain the database connection string
        e.cmd = '[redacted]'
        logger.error(e)
        # temp file contains '\copy ([SQL]) To STDOUT with CSV HEADER' so the SQL is 7 chars in up to the last 27 chars
        sql = subprocess.check_output(['cat', temp_sql_file_path]).decode()[7:-27]
        logger.error('Faulty SQL: {}'.format(sql))
        raise e
    except Exception as e:
        logger.error(e)
        # temp file contains '\copy ([SQL]) To STDOUT with CSV HEADER' so the SQL is 7 chars in up to the last 27 chars
        sql = subprocess.check_output(['cat', temp_sql_file_path]).decode()[7:-27]
        logger.error('Faulty SQL: {}'.format(sql))
        raise e


def retrieve_db_string():
    """It is necessary for this to be a function so the test suite can mock the connection string"""
    return os.environ['DOWNLOAD_DATABASE_URL']


def strip_file_extension(file_name):
    return os.path.splitext(os.path.basename(file_name))[0]


def fail_download(download_job, exception, message):
    stack_trace = "".join(
        traceback.format_exception(etype=type(exception), value=exception, tb=exception.__traceback__)
    )
    download_job.error_message = '{}:\n{}'.format(message, stack_trace)
    download_job.job_status_id = JOB_STATUS_DICT['failed']
    download_job.save()
