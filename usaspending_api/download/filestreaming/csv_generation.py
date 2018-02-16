import boto
import logging
import math
import mimetypes
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
from filechunkio import FileChunkIO

from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping, award_assistance_mapping
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers import generate_raw_quoted_query
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
        self.human_names = download_column_historical_lookups.human_names[
            model_type][file_type]
        self.query_paths = download_column_historical_lookups.query_paths[
            model_type][file_type]
        self.queryset = None

    def values(self, header):
        query_paths = [self.query_paths[hn] for hn in header]
        return self.queryset.values_list(query_paths).iterator()

    def columns(self, requested):
        """Given a list of column names requested, returns the ones available in the source"""

        if requested:
            result = []
            for column in requested:
                if column in self.human_names:
                    result.append(column)
        else:
            result = self.human_names

        # remove headers that we don't have a query path for
        result = [h for h in result if h in self.query_paths]

        return result

    def row_emitter(self, headers_requested):
        headers = self.columns(headers_requested)
        # Not yieling headers as the files can be split
        # yield headers
        query_paths = [self.query_paths[hn] for hn in headers]
        return self.queryset.values(*query_paths)


def generate_csvs(download_job, sqs_message=None):
    """Derive the relevant file location and write CSVs to it"""
    start_time = time.time()

    # Parse data from download_job
    file_name = download_job.file_name
    json_request = download_job.json_request
    columns = json_request.get('columns', None)

    # Update job attributes
    download_job.job_status_id = JOB_STATUS_DICT['running']
    download_job.number_of_rows = 0
    download_job.number_of_columns = 0
    download_job.file_size = 0
    download_job.save()

    logger.info('Starting to process Job: {}\nFilename: {}\nRequest Params: {}'.
                format(download_job.bulk_download_job_id, download_job.file_name, download_job.json_request))
    try:
        # Create temporary files and working directory
        file_path = settings.BULK_DOWNLOAD_LOCAL_PATH + file_name
        working_dir = os.path.splitext(file_path)[0]
        if not os.path.exists(working_dir):
            os.mkdir(working_dir)
        zipped_csvs = zipfile.ZipFile(file_path, 'w', allowZip64=True)

        logger.info('Generating {}'.format(file_name))

        # Generate sources from the JSON request object
        sources = get_csv_sources(json_request)
        for source in sources:
            # Write data to the file
            download_job.number_of_columns = max(download_job.number_of_columns, len(source.columns(columns)))
            parse_source(source, columns, download_job, working_dir, start_time, sqs_message, zipped_csvs)

        # Remove temporary files and working directory
        shutil.rmtree(working_dir)
        zipped_csvs.close()
        download_job.file_size = os.stat(file_path).st_size
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)

        # Set error message; job_status_id will be set in generate_bulk_zip.handle()
        download_job.error_message = 'An exception was raised while attempting to write the CSV:\n' + str(e)
        download_job.save()
        raise Exception(download_job.error_message)

    try:
        # push file to S3 bucket, if not local
        if not settings.IS_LOCAL:
            bucket = settings.BULK_DOWNLOAD_S3_BUCKET_NAME
            region = settings.BULK_DOWNLOAD_AWS_REGION
            start_uploading = time.time()
            upload(bucket, region, file_path, os.path.basename(file_path), acl='public-read',
                   parallel_processes=multiprocessing.cpu_count())
            logger.info('uploading took {} seconds'.format(time.time() - start_uploading))
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)

        # Set error message; job_status_id will be set in generate_bulk_zip.handle()
        download_job.error_message = 'An exception was raised while attempting to write the CSV:\n' + str(e)
        download_job.save()
        raise Exception(download_job.error_message)

    download_job.job_status_id = JOB_STATUS_DICT['finished']
    download_job.save()

    logger.info('Processed Job: {}\nFilename: {}\nRequest Params: {}'.
                format(download_job.bulk_download_job_id, download_job.file_name, download_job.json_request))

    return file_name


def get_csv_sources(self, json_request):
    csv_sources = []
    for award_level in json_request['award_levels']:
        queryset = VALUE_MAPPINGS[award_level]['filter_function'](json_request['filters'])
        award_level_table = VALUE_MAPPINGS[award_level]['table']

        award_type_codes = set(json_request['filters']['award_type_codes'])
        d1_award_types = set(contract_type_mapping.keys())
        d2_award_types = set(award_assistance_mapping.keys())
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


def parse_source(source, columns, download_job, working_dir, start_time, message, zipped_csvs):
    """Write to csv and zip files using the source data"""
    source_map = {'prime_awards': 'awards', 'sub_awards': "subawards"}
    d_map = {'d1': 'contracts', 'd2': 'assistance'}

    source_name = '{}_{}'.format(source_map[source.source_type], d_map[source.file_type])
    source_query = source.row_emitter(columns)

    reached_end = False
    split_csv = 1
    while not reached_end:
        start_split_writing = time.time()
        split_csv_name = '{}_{}.csv'.format(source_name, split_csv)
        split_csv_path = os.path.join(working_dir, split_csv_name)

        # Generate the final query, values, limits, dates fixed
        csv_query_split = source_query[(split_csv - 1) * EXCEL_ROW_LIMIT:split_csv * EXCEL_ROW_LIMIT]
        csv_query_raw = generate_raw_quoted_query(csv_query_split)
        csv_query_annotated = apply_annotations_to_sql(csv_query_raw, source.human_names)
        logger.debug('PSQL Query: {}'.format(csv_query_annotated))
        csv_query_cmd = '\copy ({}) To STDOUT with CSV HEADER'.format(csv_query_annotated)

        # Create a unique temporary file to hold the raw query
        (temp_sql_file, temp_sql_file_path) = tempfile.mkstemp(prefix='bd_sql_', dir='/tmp')
        with open(temp_sql_file_path, 'w') as file:
            file.write(csv_query_cmd)

        # Run the PSQL command as a separate Process
        psql_process = multiprocessing.Process(target=execute_psql, args=(temp_sql_file_path, split_csv_path,))
        psql_process.start()

        # Let the thread run until it finishes (max MAX_VISIBILITY_TIMEOUT), with a buffer of
        # DOWNLOAD_VISIBILITY_TIMEOUT
        while psql_process.is_alive() and (time.time() - start_time) < MAX_VISIBILITY_TIMEOUT:
            if message:
                message.change_visibility(VisibilityTimeout=DOWNLOAD_VISIBILITY_TIMEOUT)
            time.sleep(60)

        if (time.time() - start_time) >= MAX_VISIBILITY_TIMEOUT:
            # Process is running for longer than MAX_VISIBILITY_TIMEOUT, kill it
            logger.error('Attempting to terminate process (pid {})'.format(psql_process.pid))
            psql_process.terminate()

            # Remove all temp files
            os.close(temp_sql_file)
            os.remove(temp_sql_file_path)
            shutil.rmtree(working_dir)
            zipped_csvs.close()
            raise TimeoutError('Bulk download job lasted longer than {} hours'.format(str(MAX_VISIBILITY_TIMEOUT/3600)))

        # Write it to the zip
        zipped_csvs.write(split_csv_path, split_csv_name)
        logger.info('Wrote {}, took {} seconds'.format(split_csv_name, time.time() - start_split_writing))

        # Remove temporary files
        os.close(temp_sql_file)
        os.remove(temp_sql_file_path)

        last_count = len(open(split_csv_path).readlines())
        if last_count < EXCEL_ROW_LIMIT + 1:
            # Will be hit when line 201 ((split_csv - 1) * EXCEL_ROW_LIMIT) > number of rows in source
            download_job.number_of_rows += EXCEL_ROW_LIMIT * (split_csv - 1) + last_count
            reached_end = True
        else:
            split_csv += 1


# Multipart upload functions copied from Fabian Topfstedt's solution
# http://www.topfstedt.de/python-parallel-s3-multipart-upload-with-retries.html
def upload(bucketname, regionname, source_path, keyname, acl='private', headers={}, guess_mimetype=True,
           parallel_processes=4):
    """
    Parallel multipart upload.
    """
    bucket = boto.s3.connect_to_region(regionname).get_bucket(bucketname)
    if guess_mimetype:
        mtype = mimetypes.guess_type(keyname)[0] or 'application/octet-stream'
        headers.update({'Content-Type': mtype})

    mp = bucket.initiate_multipart_upload(keyname, headers=headers)

    source_size = os.stat(source_path).st_size
    bytes_per_chunk = max(int(math.sqrt(5242880) * math.sqrt(source_size)),
                          5242880)
    chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))

    pool = multiprocessing.Pool(processes=parallel_processes)
    for i in range(chunk_amount):
        offset = i * bytes_per_chunk
        remaining_bytes = source_size - offset
        bytes = min([bytes_per_chunk, remaining_bytes])
        part_num = i + 1
        pool.apply_async(_upload_part, [bucketname, regionname, mp.id,
                         part_num, source_path, offset, bytes])
    pool.close()
    pool.join()

    if len(mp.get_all_parts()) == chunk_amount:
        mp.complete_upload()
        key = bucket.get_key(keyname)
        key.set_acl(acl)
    else:
        mp.cancel_upload()


def _upload_part(bucketname, regionname, multipart_id, part_num, source_path, offset, bytes, amount_of_retries=10):
    """Uploads a part with retries."""
    bucket = boto.s3.connect_to_region(regionname).get_bucket(bucketname)

    def _upload(retries_left=amount_of_retries):
        try:
            logging.info('Start uploading part #%d ...' % part_num)
            for mp in bucket.get_all_multipart_uploads():
                if mp.id == multipart_id:
                    with FileChunkIO(source_path, 'r', offset=offset,
                                     bytes=bytes) as fp:
                        mp.upload_part_from_file(fp=fp, part_num=part_num)
                    break
        except Exception as exc:
            if retries_left:
                _upload(retries_left=retries_left - 1)
            else:
                logging.info('... Failed uploading part #%d' % part_num)
                raise exc
        else:
            logging.info('... Uploaded part #%d' % part_num)
    _upload()


def verify_requested_columns_available(sources, requested):
    bad_cols = set(requested)
    for source in sources:
        bad_cols -= set(source.columns(requested))
    if bad_cols:
        raise InvalidParameterException('Unknown columns: {}'.format(bad_cols))


def apply_annotations_to_sql(raw_query, aliases):
    """
    Django's ORM understandably doesn't allow aliases to be the same names
    as other fields available. However, if we want to use the efficiency of
    psql's \copy method and keep the column names, we need to allow these
    scenarios. This function simply outputs a modified raw sql which
    does the aliasing, allowing these scenarios.
    """
    select_string = re.findall('SELECT (.*) FROM', raw_query)[0]
    selects = [select.strip() for select in select_string.split(',')]
    if len(selects) != len(aliases):
        raise Exception("Length of alises doesn't match the columns in selects")
    selects_mapping = OrderedDict(zip(aliases, selects))
    new_select_string = ", ".join(['{} AS \"{}\"'.format(select, alias)
                                   for alias, select in selects_mapping.items()])
    return raw_query.replace(select_string, new_select_string)


def execute_psql(temp_sql_file_path, split_csv_path):
    try:
        # Generate the csv with \copy
        cat_command = subprocess.Popen(['cat', temp_sql_file_path], stdout=subprocess.PIPE)
        subprocess.call(['psql', '-o', split_csv_path, os.environ['DOWNLOAD_DATABASE_URL']], stdin=cat_command.stdout)
    except Exception as e:
        logger.error(str(e))
        raise e
