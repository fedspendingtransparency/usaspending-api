import json
import logging
import multiprocessing
import os
import re
import shutil
import subprocess
import tempfile
import time
import zipfile
import csv

from django.conf import settings

from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping, assistance_type_mapping
from usaspending_api.common.helpers.generic_helper import generate_raw_quoted_query
from usaspending_api.download.helpers import (verify_requested_columns_available, multipart_upload, split_csv,
                                              write_to_download_log as write_to_log)
from usaspending_api.download.filestreaming.csv_source import CsvSource
from usaspending_api.download.lookups import JOB_STATUS_DICT, VALUE_MAPPINGS

DOWNLOAD_VISIBILITY_TIMEOUT = 60*10
MAX_VISIBILITY_TIMEOUT = 60*60*4
EXCEL_ROW_LIMIT = 1000000
WAIT_FOR_PROCESS_SLEEP = 5

logger = logging.getLogger('console')


def generate_csvs(download_job, sqs_message=None):
    """Derive the relevant file location and write CSVs to it"""
    start_time = time.time()

    # Parse data from download_job
    json_request = json.loads(download_job.json_request)
    columns = json_request.get('columns', None)
    limit = json_request.get('limit', None)

    file_name = start_download(download_job)
    try:
        # Create temporary files and working directory
        file_path = settings.CSV_LOCAL_PATH + file_name
        working_dir = os.path.splitext(file_path)[0]
        if not os.path.exists(working_dir):
            os.mkdir(working_dir)

        write_to_log(message='Generating {}'.format(file_name), download_job=download_job)

        # Generate sources from the JSON request object
        sources = get_csv_sources(json_request)
        for source in sources:
            # Parse and write data to the file
            download_job.number_of_columns = max(download_job.number_of_columns, len(source.columns(columns)))
            parse_source(source, columns, download_job, working_dir, start_time, sqs_message, file_path, limit)
        download_job.file_size = os.stat(file_path).st_size
    except Exception as e:
        # Set error message; job_status_id will be set in generate_zip.handle()
        download_job.error_message = 'An exception was raised while attempting to write the file:\n{}'.format(str(e))
        download_job.save()
        raise type(e)(download_job.error_message)
    finally:
        # Remove working directory
        if os.path.exists(working_dir):
            shutil.rmtree(working_dir)

    try:
        # push file to S3 bucket, if not local
        if not settings.IS_LOCAL:
            bucket = settings.BULK_DOWNLOAD_S3_BUCKET_NAME
            region = settings.USASPENDING_AWS_REGION
            start_uploading = time.time()
            multipart_upload(bucket, region, file_path, os.path.basename(file_path),
                             parallel_processes=multiprocessing.cpu_count())
            write_to_log(message='Uploading took {} seconds'.format(time.time() - start_uploading),
                         download_job=download_job)
    except Exception as e:
        # Set error message; job_status_id will be set in generate_zip.handle()
        download_job.error_message = 'An exception was raised while attempting to upload the file:\n{}'.format(str(e))
        download_job.save()
        raise type(e)(download_job.error_message)
    finally:
        # Remove generated file
        if not settings.IS_LOCAL and os.path.exists(file_path):
            os.remove(file_path)

    return finish_download(download_job)


def get_csv_sources(json_request):
    csv_sources = []
    for download_type in json_request['download_types']:
        agency_id = json_request['filters'].get('agency', 'all')
        filter_function = VALUE_MAPPINGS[download_type]['filter_function']
        download_type_table = VALUE_MAPPINGS[download_type]['table']

        if VALUE_MAPPINGS[download_type]['source_type'] == 'award':
            queryset = filter_function(json_request['filters'])

            # Award downloads
            queryset = filter_function(json_request['filters'])
            award_type_codes = set(json_request['filters']['award_type_codes'])
            d1_award_type_codes = set(contract_type_mapping.keys())
            d2_award_type_codes = set(assistance_type_mapping.keys())

            if award_type_codes & d1_award_type_codes:
                # only generate d1 files if the user is asking for contract data
                d1_source = CsvSource(VALUE_MAPPINGS[download_type]['table_name'], 'd1', download_type, agency_id)
                d1_filters = {'{}__isnull'.format(VALUE_MAPPINGS[download_type]['contract_data']): False}
                d1_source.queryset = queryset & download_type_table.objects.filter(**d1_filters)
                csv_sources.append(d1_source)

            if award_type_codes & d2_award_type_codes:
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


def parse_source(source, columns, download_job, working_dir, start_time, message, zipfile_path, limit):
    """Write to csv and zip files using the source data"""
    d_map = {'d1': 'contracts', 'd2': 'assistance', 'treasury_account': 'treasury_account',
             'federal_account': 'federal_account'}
    source_name = '{}_{}_{}'.format(source.agency_code, d_map[source.file_type],
                                    VALUE_MAPPINGS[source.source_type]['download_name'])
    source_query = source.row_emitter(columns)
    source_path = os.path.join(working_dir, '{}.csv'.format(source_name))

    # Generate the query file; values, limits, dates fixed
    temp_file, temp_file_path = generate_temp_query_file(source_query, limit, source, download_job, columns)

    start_time = time.time()
    try:
        # Create a separate process to run the PSQL command; wait
        psql_process = multiprocessing.Process(target=execute_psql, args=(temp_file_path, source_path, download_job,))
        psql_process.start()
        wait_for_process(psql_process, start_time, download_job, message)

        # The process below modifies the download job and thus cannot be in a separate process
        # Assuming the process to count the number of lines in a CSV file takes less than DOWNLOAD_VISIBILITY_TIMEOUT
        #  in case the visibility times out before then
        if message:
            message.change_visibility(VisibilityTimeout=DOWNLOAD_VISIBILITY_TIMEOUT)
        # Log how many rows we have
        with open(source_path, 'r') as source_csv:
            download_job.number_of_rows += sum(1 for row in csv.reader(source_csv)) - 1
            download_job.save()

        # Create a separate process to split the large csv into smaller csvs and write to zip; wait
        zip_process = multiprocessing.Process(target=split_and_zip_csvs, args=(zipfile_path, source_path, source_name,
                                                                               download_job,))
        zip_process.start()
        wait_for_process(zip_process, start_time, download_job, message)
        download_job.save()
    except Exception as e:
        raise e
    finally:
        # Remove temporary files
        os.close(temp_file)
        os.remove(temp_file_path)


def split_and_zip_csvs(zipfile_path, source_path, source_name, download_job=None):
    try:
        # Split CSV into separate files
        log_time = time.time()
        if download_job:
            # If detailed name exists, use existing name instead of output_name_template
            split_csvs = split_csv(source_path, row_limit=EXCEL_ROW_LIMIT, output_path=os.path.dirname(source_path),
                                   file_name=download_job.file_name)
            write_to_log(message='Splitting csvs took {} seconds'.format(time.time() - log_time),
                         download_job=download_job)
        else:
            split_csvs = split_csv(source_path, row_limit=EXCEL_ROW_LIMIT, output_path=os.path.dirname(source_path),
                                   output_name_template='{}_%s.csv'.format(source_name))

        # Zip the split CSVs into one zipfile
        log_time = time.time()
        zipped_csvs = zipfile.ZipFile(zipfile_path, 'a', compression=zipfile.ZIP_DEFLATED, allowZip64=True)
        for split_csv_part in split_csvs:
            zipped_csvs.write(split_csv_part, os.path.basename(split_csv_part))

        if download_job:
            write_to_log(message='Writing to zipfile took {} seconds'.format(time.time() - log_time),
                         download_job=download_job)
    except Exception as e:
        logger.error(e)
        raise e
    finally:
        if zipped_csvs:
            zipped_csvs.close()


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


def wait_for_process(process, start_time, download_job, message):
    """Wait for the process to complete, throw errors for timeouts or Process exceptions"""
    log_time = time.time()

    # Let the thread run until it finishes (max MAX_VISIBILITY_TIMEOUT), with a buffer of DOWNLOAD_VISIBILITY_TIMEOUT
    sleep_count = 0
    while process.is_alive() and (time.time() - start_time) < MAX_VISIBILITY_TIMEOUT:
        if message:
            message.change_visibility(VisibilityTimeout=DOWNLOAD_VISIBILITY_TIMEOUT)

        if sleep_count < 10:
            time.sleep(WAIT_FOR_PROCESS_SLEEP/5)
        else:
            time.sleep(WAIT_FOR_PROCESS_SLEEP)
        sleep_count += 1

    if (time.time() - start_time) >= MAX_VISIBILITY_TIMEOUT or process.exitcode != 0:
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

    return time.time() - log_time


def generate_temp_query_file(source_query, limit, source, download_job, columns):
    if limit:
        source_query = source_query[:limit]
    csv_query_raw = generate_raw_quoted_query(source_query)
    csv_query_annotated = apply_annotations_to_sql(csv_query_raw, source.columns(columns))

    write_to_log(message='Creating PSQL Query: {}'.format(csv_query_annotated), download_job=download_job,
                 is_debug=True)

    # Create a unique temporary file to hold the raw query
    (temp_sql_file, temp_sql_file_path) = tempfile.mkstemp(prefix='bd_sql_', dir='/tmp')
    with open(temp_sql_file_path, 'w') as file:
        file.write('\copy ({}) To STDOUT with CSV HEADER'.format(csv_query_annotated))

    return temp_sql_file, temp_sql_file_path


def apply_annotations_to_sql(raw_query, aliases):
    """
    Django's ORM understandably doesn't allow aliases to be the same names as other fields available. However, if we
    want to use the efficiency of psql's \copy method and keep the column names, we need to allow these scenarios. This
    function simply outputs a modified raw sql which does the aliasing, allowing these scenarios.
    """
    aliases_copy = list(aliases)

    # Extract everything between the first SELECT and the last FROM
    query_before_group_by = raw_query.split('GROUP BY ')[0]
    query_before_from = re.sub("SELECT ", "", ' FROM'.join(re.split(' FROM', query_before_group_by)[:-1]), count=1)

    # Create a list from the non-derived values between SELECT and FROM
    selects_str = re.findall('SELECT (.*?) (CASE|CONCAT|SUM|\(SELECT|FROM)', raw_query)[0]
    just_selects = selects_str[0] if selects_str[1] == 'FROM' else selects_str[0][:-1]
    selects_list = [select.strip() for select in just_selects.strip().split(',')]

    # Create a list from the derived values between SELECT and FROM
    remove_selects = query_before_from.replace(selects_str[0], "")
    deriv_str_lookup = re.findall('(CASE|CONCAT|SUM|\(SELECT|)(.*?) AS (.*?)( |$)', remove_selects)
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
        # Generate the csv with \copy
        log_time = time.time()

        cat_command = subprocess.Popen(['cat', temp_sql_file_path], stdout=subprocess.PIPE)
        subprocess.check_output(['psql', '-o', source_path, retrieve_db_string(), '-v', 'ON_ERROR_STOP=1'],
                                stdin=cat_command.stdout, stderr=subprocess.STDOUT)

        write_to_log(message='Wrote {}, took {} seconds'.format(os.path.basename(source_path), time.time() - log_time),
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
