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

from collections import OrderedDict
from django.conf import settings

from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping, assistance_type_mapping
from usaspending_api.common.helpers import generate_raw_quoted_query
from usaspending_api.download.helpers import (verify_requested_columns_available, multipart_upload,
                                              write_to_download_log as write_to_log)
from usaspending_api.download.lookups import JOB_STATUS_DICT, VALUE_MAPPINGS
from usaspending_api.download.v2 import download_column_historical_lookups

DOWNLOAD_VISIBILITY_TIMEOUT = 60*10
MAX_VISIBILITY_TIMEOUT = 60*60*4
BUFFER_SIZE = (5 * 1024 ** 2)
EXCEL_ROW_LIMIT = 1000000

logger = logging.getLogger('console')


class CsvSource:
    def __init__(self, model_type, file_type, source_type):
        self.model_type = model_type
        self.file_type = file_type
        self.source_type = source_type
        self.human_names = download_column_historical_lookups.human_names[model_type][file_type]
        self.query_paths = download_column_historical_lookups.query_paths[model_type][file_type]
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
        file_path = settings.BULK_DOWNLOAD_LOCAL_PATH + file_name
        working_dir = os.path.splitext(file_path)[0]
        if not os.path.exists(working_dir):
            os.mkdir(working_dir)
        zipped_csvs = zipfile.ZipFile(file_path, 'w', allowZip64=True)

        write_to_log(message='Generating {}'.format(file_name), download_job=download_job)

        # Generate sources from the JSON request object
        sources = get_csv_sources(json_request)
        for source in sources:
            # Parse and write data to the file
            download_job.number_of_columns = max(download_job.number_of_columns, len(source.columns(columns)))
            parse_source(source, columns, download_job, working_dir, start_time, sqs_message, zipped_csvs, limit)

        # Remove temporary files and working directory
        shutil.rmtree(working_dir)
        zipped_csvs.close()
        download_job.file_size = os.stat(file_path).st_size
    except Exception as e:
        logger.error(e)
        handle_file_generation_exception(file_path, download_job, 'write', str(e))

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
        logger.error(e)
        handle_file_generation_exception(file_path, download_job, 'upload', str(e))

    return finish_download(download_job)


def get_csv_sources(json_request):
    csv_sources = []
    for award_level in json_request['award_levels']:
        queryset = VALUE_MAPPINGS[award_level]['filter_function'](json_request['filters'])
        award_level_table = VALUE_MAPPINGS[award_level]['table']

        award_type_codes = set(json_request['filters']['award_type_codes'])
        d1_award_types = set(contract_type_mapping.keys())
        d2_award_types = set(assistance_type_mapping.keys())

        if award_type_codes & d1_award_types:
            # only generate d1 files if the user is asking for contracts
            d1_source = CsvSource(VALUE_MAPPINGS[award_level]['table_name'], 'd1', award_level)
            d1_filters = {'{}__isnull'.format(VALUE_MAPPINGS[award_level]['contract_data']): False}
            d1_source.queryset = queryset & award_level_table.objects.filter(**d1_filters)
            csv_sources.append(d1_source)

        if award_type_codes & d2_award_types:
            # only generate d2 files if the user is asking for assistance data
            d2_source = CsvSource(VALUE_MAPPINGS[award_level]['table_name'], 'd2', award_level)
            d2_filters = {'{}__isnull'.format(VALUE_MAPPINGS[award_level]['assistance_data']): False}
            d2_source.queryset = queryset & award_level_table.objects.filter(**d2_filters)
            csv_sources.append(d2_source)

        verify_requested_columns_available(tuple(csv_sources), json_request['columns'])

    return csv_sources


def parse_source(source, columns, download_job, working_dir, start_time, message, zipped_csvs, limit):
    """Write to csv and zip files using the source data"""
    d_map = {'d1': 'contracts', 'd2': 'assistance'}
    source_name = '{}_{}'.format(VALUE_MAPPINGS[source.source_type]['download_name'], d_map[source.file_type])
    source_query = source.row_emitter(columns)

    reached_end = False
    split_csv = 1
    while not reached_end:
        start_split_writing = time.time()
        split_csv_name = '{}_{}.csv'.format(source_name, split_csv)
        split_csv_path = os.path.join(working_dir, split_csv_name)

        # Generate the query file; values, limits, dates fixed
        temp_file, temp_file_path = generate_temp_query_file(split_csv, source_query, limit, source, download_job)

        # Run the PSQL command as a separate Process
        psql_process = multiprocessing.Process(target=execute_psql, args=(temp_file_path, split_csv_path,))
        psql_process.start()

        # Let the thread run until it finishes (max MAX_VISIBILITY_TIMEOUT), with a buffer of
        # DOWNLOAD_VISIBILITY_TIMEOUT
        while psql_process.is_alive() and (time.time() - start_time) < MAX_VISIBILITY_TIMEOUT:
            if message:
                message.change_visibility(VisibilityTimeout=DOWNLOAD_VISIBILITY_TIMEOUT)
            time.sleep(60)

        if (time.time() - start_time) >= MAX_VISIBILITY_TIMEOUT:
            # Process is running for longer than MAX_VISIBILITY_TIMEOUT, kill it
            write_to_log(message='Attempting to terminate process (pid {})'.format(psql_process.pid),
                         download_job=download_job, is_error=True)
            psql_process.terminate()

            # Remove all temp files
            os.close(temp_file)
            os.remove(temp_file_path)
            shutil.rmtree(working_dir)
            zipped_csvs.close()
            raise TimeoutError('Download job lasted longer than {} hours'.format(str(MAX_VISIBILITY_TIMEOUT/3600)))

        # Write it to the zip
        zipped_csvs.write(split_csv_path, split_csv_name)
        write_to_log(message='Wrote {}, took {} seconds'.format(split_csv_name, time.time() - start_split_writing),
                     download_job=download_job)

        # Remove temporary files
        os.close(temp_file)
        os.remove(temp_file_path)

        last_count = len(open(split_csv_path).readlines())
        if last_count <= EXCEL_ROW_LIMIT:
            # Will be hit when line 201 ((split_csv - 1) * EXCEL_ROW_LIMIT) > number of rows in source
            download_job.number_of_rows += EXCEL_ROW_LIMIT * (split_csv - 1) + last_count - 1
            reached_end = True
        else:
            split_csv += 1


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

    write_to_log(message='Finished to processing DownloadJob {}'.format(download_job.download_job_id),
                 download_job=download_job)

    return download_job.file_name


def generate_temp_query_file(split_csv, source_query, limit, source, download_job):
    csv_limit = split_csv * EXCEL_ROW_LIMIT
    csv_query_split = source_query[csv_limit - EXCEL_ROW_LIMIT:csv_limit if (not limit or csv_limit < limit) else limit]
    csv_query_raw = generate_raw_quoted_query(csv_query_split)
    csv_query_annotated = apply_annotations_to_sql(csv_query_raw, source.human_names)

    write_to_log(message='Creating PSQL Query: {}'.format(csv_query_annotated), download_job=download_job,
                 is_debug=True)

    # Create a unique temporary file to hold the raw query
    (temp_sql_file, temp_sql_file_path) = tempfile.mkstemp(prefix='bd_sql_', dir='/tmp')
    with open(temp_sql_file_path, 'w') as file:
        file.write('\copy ({}) To STDOUT with CSV HEADER'.format(csv_query_annotated))

    return temp_sql_file, temp_sql_file_path


def handle_file_generation_exception(file_path, download_job, error_type, error):
    """Removes the temporary file, updates the job, and raises the exception"""
    logger.error(error)
    if os.path.exists(file_path):
        os.remove(file_path)

    # Set error message; job_status_id will be set in generate_zip.handle()
    download_job.error_message = 'An exception was raised while attempting to {} the CSV:\n{}'.format(error_type, error)
    download_job.save()

    raise Exception(download_job.error_message)


def apply_annotations_to_sql(raw_query, aliases):
    """
    Django's ORM understandably doesn't allow aliases to be the same names as other fields available. However, if we
    want to use the efficiency of psql's \copy method and keep the column names, we need to allow these scenarios. This
    function simply outputs a modified raw sql which does the aliasing, allowing these scenarios.
    """
    select_string = re.findall('SELECT (.*) FROM', raw_query)[0]
    selects = [select.strip() for select in select_string.split(',')]
    if len(selects) != len(aliases):
        raise Exception("Length of alises doesn't match the columns in selects")
    selects_mapping = OrderedDict(zip(aliases, selects))
    new_select_string = ", ".join(['{} AS \"{}\"'.format(select, alias) for alias, select in selects_mapping.items()])
    return raw_query.replace(select_string, new_select_string)


def execute_psql(temp_sql_file_path, split_csv_path):
    """Executes a single PSQL command within its own Subprocess"""
    try:
        # Generate the csv with \copy
        cat_command = subprocess.Popen(['cat', temp_sql_file_path], stdout=subprocess.PIPE)
        subprocess.call(['psql', '-o', split_csv_path, os.environ['DOWNLOAD_DATABASE_URL']], stdin=cat_command.stdout)
    except Exception as e:
        logger.error(e)
        raise e
