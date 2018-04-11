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

from collections import OrderedDict
from django.conf import settings

from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping, assistance_type_mapping
from usaspending_api.common.helpers import generate_raw_quoted_query
from usaspending_api.download.helpers import (verify_requested_columns_available, multipart_upload, split_csv,
                                              write_to_download_log as write_to_log)
from usaspending_api.download.lookups import JOB_STATUS_DICT, VALUE_MAPPINGS
from usaspending_api.download.v2 import download_column_historical_lookups

DOWNLOAD_VISIBILITY_TIMEOUT = 60*10
MAX_VISIBILITY_TIMEOUT = 60*60*4
BUFFER_SIZE = (5 * 1024 ** 2)
EXCEL_ROW_LIMIT = 1000000
WAIT_FOR_PROCESS_SLEEP = 5

logger = logging.getLogger('console')


class CsvSource:
    def __init__(self, model_type, file_type, source_type):
        self.model_type = model_type
        self.file_type = file_type
        self.source_type = source_type
        self.query_paths = download_column_historical_lookups.query_paths[model_type][file_type]
        self.human_names = list(self.query_paths.keys())
        self.queryset = None

    def values(self, header):
        query_paths = [self.query_paths[hn] for hn in header]
        return self.queryset.values_list(query_paths).iterator()

    def columns(self, requested):
        """Given a list of column names requested, returns the ones available in the source"""
        result = self.human_names
        if requested:
            result = [header for header in requested if header in self.human_names]

        # remove headers that we don't have a query path for
        result = [header for header in result if header in self.query_paths]

        return result

    def row_emitter(self, headers_requested):
        headers = self.columns(headers_requested)
        # Not yielding headers as the files can be split
        # yield headers
        query_paths = [self.query_paths[hn] for hn in headers]
        return self.queryset.values(*query_paths)


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
        download_job.error_message = 'An exception was raised while attempting to write the file:\n{}'.format(e)
        download_job.save()
        raise Exception(download_job.error_message)
    finally:
        # Remove working directory
        if os.path.exists(working_dir):
            shutil.rmtree(working_dir)

    try:
        # push file to S3 bucket, if not local
        if not settings.IS_LOCAL:
            bucket = settings.BULK_DOWNLOAD_S3_BUCKET_NAME
            region = settings.BULK_DOWNLOAD_AWS_REGION
            start_uploading = time.time()
            multipart_upload(bucket, region, file_path, os.path.basename(file_path), acl='public-read',
                             parallel_processes=multiprocessing.cpu_count())
            write_to_log(message='Uploading took {} seconds'.format(time.time() - start_uploading),
                         download_job=download_job)
    except Exception as e:
        # Set error message; job_status_id will be set in generate_zip.handle()
        download_job.error_message = 'An exception was raised while attempting to upload the file:\n{}'.format(e)
        download_job.save()
        raise Exception(download_job.error_message)
    finally:
        # Remove generated file
        if not settings.IS_LOCAL and os.path.exists(file_path):
            os.remove(file_path)

    return finish_download(download_job)


def get_csv_sources(json_request):
    csv_sources = []
    for award_level in json_request['award_levels']:
        queryset = VALUE_MAPPINGS[award_level]['filter_function'](json_request['filters'])
        award_level_table = VALUE_MAPPINGS[award_level]['table']

        award_type_codes = set(json_request['filters']['award_type_codes'])
        d1_award_type_codes = set(contract_type_mapping.keys())
        d2_award_type_codes = set(assistance_type_mapping.keys())

        if award_type_codes & d1_award_type_codes:
            # only generate d1 files if the user is asking for contract data
            d1_source = CsvSource(VALUE_MAPPINGS[award_level]['table_name'], 'd1', award_level)
            d1_filters = {'{}__isnull'.format(VALUE_MAPPINGS[award_level]['contract_data']): False}
            d1_source.queryset = queryset & award_level_table.objects.filter(**d1_filters)
            csv_sources.append(d1_source)

        if award_type_codes & d2_award_type_codes:
            # only generate d2 files if the user is asking for assistance data
            d2_source = CsvSource(VALUE_MAPPINGS[award_level]['table_name'], 'd2', award_level)
            d2_filters = {'{}__isnull'.format(VALUE_MAPPINGS[award_level]['assistance_data']): False}
            d2_source.queryset = queryset & award_level_table.objects.filter(**d2_filters)
            csv_sources.append(d2_source)

        verify_requested_columns_available(tuple(csv_sources), json_request.get('columns', []))

    return csv_sources


def parse_source(source, columns, download_job, working_dir, start_time, message, zipfile_path, limit):
    """Write to csv and zip files using the source data"""
    d_map = {'d1': 'contracts', 'd2': 'assistance'}
    source_name = '{}_{}'.format(d_map[source.file_type], VALUE_MAPPINGS[source.source_type]['download_name'])
    source_query = source.row_emitter(columns)
    source_path = os.path.join(working_dir, '{}.csv'.format(source_name))

    # Generate the query file; values, limits, dates fixed
    temp_file, temp_file_path = generate_temp_query_file(source_query, limit, source, download_job)

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


def split_and_zip_csvs(zipfile_path, source_path, source_name, download_job):
    try:
        # Split CSV into separate files
        log_time = time.time()
        split_csvs = split_csv(source_path, row_limit=EXCEL_ROW_LIMIT, output_path=os.path.dirname(source_path),
                               output_name_template='{}_%s.csv'.format(source_name))
        write_to_log(message='Splitting csvs took {} seconds'.format(time.time() - log_time), download_job=download_job)

        # Zip the split CSVs into one zipfile
        log_time = time.time()
        zipped_csvs = zipfile.ZipFile(zipfile_path, 'a', compression=zipfile.ZIP_DEFLATED, allowZip64=True)
        for split_csv_part in split_csvs:
            zipped_csvs.write(split_csv_part, os.path.basename(split_csv_part))

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
    while process.is_alive() and (time.time() - start_time) < MAX_VISIBILITY_TIMEOUT:
        if message:
            message.change_visibility(VisibilityTimeout=DOWNLOAD_VISIBILITY_TIMEOUT)
        time.sleep(WAIT_FOR_PROCESS_SLEEP)

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


def generate_temp_query_file(source_query, limit, source, download_job):
    if limit:
        source_query = source_query[:limit]
    csv_query_raw = generate_raw_quoted_query(source_query)
    csv_query_annotated = apply_annotations_to_sql(csv_query_raw, source.human_names)

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
    select_string = re.findall('SELECT (.*?) FROM', raw_query)[0]
    selects = [select.strip() for select in select_string.split(',')]
    if len(selects) != len(aliases):
        raise Exception("Length of alises doesn't match the columns in selects")
    selects_mapping = OrderedDict(zip(aliases, selects))
    new_select_string = ", ".join(['{} AS \"{}\"'.format(select, alias) for alias, select in selects_mapping.items()])
    return raw_query.replace(select_string, new_select_string)


def execute_psql(temp_sql_file_path, source_path, download_job):
    """Executes a single PSQL command within its own Subprocess"""
    try:
        # Generate the csv with \copy
        log_time = time.time()

        cat_command = subprocess.Popen(['cat', temp_sql_file_path], stdout=subprocess.PIPE)
        subprocess.check_output(['psql', '-o', source_path, os.environ['DOWNLOAD_DATABASE_URL'], '-v',
                                 'ON_ERROR_STOP=1'], stdin=cat_command.stdout, stderr=subprocess.STDOUT)

        write_to_log(message='Wrote {}, took {} seconds'.format(os.path.basename(source_path), time.time() - log_time),
                     download_job=download_job)
    except subprocess.CalledProcessError as e:
        # Not logging the command as it can contain the database connection string
        e.cmd = '[redacted]'
        logger.error(e)
        raise e
    except Exception as e:
        logger.error(e)
        raise e
