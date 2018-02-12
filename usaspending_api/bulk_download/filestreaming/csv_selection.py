import csv
import io
import logging
import jsonpickle
import time
import os
import zipfile
import subprocess
import re
import shutil
import math
import multiprocessing
from filechunkio import FileChunkIO
import mimetypes
from collections import OrderedDict
import tempfile

import boto
from django.conf import settings

from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.v2 import download_column_historical_lookups
from usaspending_api.common.helpers import generate_raw_quoted_query

BULK_DOWNLOAD_VISIBILITY_TIMEOUT = 60*10
MAX_VISIBILITY_TIMEOUT = 60*60*4

BUFFER_SIZE = (5 * 1024 ** 2)
EXCEL_ROW_LIMIT = 1000000

logger = logging.getLogger('console')


def update_number_of_columns(row, download_job):
    if download_job.number_of_columns is None:
        download_job.number_of_columns = len(row)
    else:
        download_job.number_of_columns = max(download_job.number_of_columns,
                                             len(row))


def csv_row_emitter(body, download_job):
    header_row = True
    for row in body:
        string_buffer = io.StringIO()
        writer = csv.writer(string_buffer)
        writer.writerow(row)
        if header_row:
            update_number_of_columns(row, download_job)
            header_row = False
        else:
            download_job.number_of_rows += 1

        yield string_buffer.getvalue().encode('utf8')


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

    def toJsonDict(self):
        json_dict = {
            'model_type': self.model_type,
            'file_type': self.file_type,
            'source_type': self.source_type,
            'query': jsonpickle.dumps(self.queryset.query) if self.queryset is not None else None
        }
        return json_dict


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


def _upload_part(bucketname, regionname, multipart_id, part_num,
                 source_path, offset, bytes, amount_of_retries=10):
    """
    Uploads a part with retries.
    """
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


def write_csvs(download_job, file_name, columns, sources, message=None):
    """Derive the relevant location and write CSVs to it.

    :return: the final file name (complete with prefix)"""
    start_zip_generation = time.time()

    download_job.job_status_id = JOB_STATUS_DICT['running']
    download_job.number_of_rows = 0
    download_job.number_of_columns = 0
    download_job.file_size = 0
    download_job.save()

    logger.info('Processing Job: {}\n'
                'Filename: {}\n'
                'Request Params: {}'.format(download_job.bulk_download_job_id,
                                            download_job.file_name,
                                            download_job.json_request))

    try:
        file_path = settings.BULK_DOWNLOAD_LOCAL_PATH + file_name
        working_dir = os.path.splitext(file_path)[0]
        if not os.path.exists(working_dir):
            os.mkdir(working_dir)
        zipped_csvs = zipfile.ZipFile(file_path, 'w', allowZip64=True)

        logger.info('Generating {}'.format(file_name))

        source_map = {'prime_awards': 'awards',
                      'sub_awards': "subawards"}
        d_map = {'d1': 'contracts',
                 'd2': 'assistance'}

        overall_start_time = time.time()

        for source in sources:
            source_name = '{}_{}'.format(source_map[source.source_type], d_map[source.file_type])

            source_query = source.row_emitter(columns)
            download_job.number_of_columns = max(download_job.number_of_columns, len(source.columns(columns)))
            reached_end = False
            split_csv = 1
            while not reached_end:
                split_csv_name = '{}_{}.csv'.format(source_name, split_csv)
                split_csv_path = os.path.join(working_dir, split_csv_name)

                start_split_writing = time.time()
                # Generate the final query, values, limits, dates fixed
                split_csv_query = source_query[(split_csv - 1) * EXCEL_ROW_LIMIT:split_csv * EXCEL_ROW_LIMIT]
                split_csv_query_raw = generate_raw_quoted_query(split_csv_query)
                split_csv_query_raw = apply_annotations_to_sql(split_csv_query_raw, source.human_names)
                logger.debug('PSQL Query: {}'.format(split_csv_query_raw))
                split_csv_query_raw = '\copy ({}) To STDOUT with CSV HEADER'.format(split_csv_query_raw)

                # Create a unique temporary file to hold the raw query
                (temp_sql_file_fd, temp_sql_file_path) = tempfile.mkstemp(prefix='bd_sql_', dir='/tmp')
                with open(temp_sql_file_path, 'w') as temp_sql_file:
                    temp_sql_file.write(split_csv_query_raw)

                # Run the psql command as a separate Process
                psql_process = multiprocessing.Process(target=execute_psql, args=(temp_sql_file_path, split_csv_path,))
                psql_process.start()

                # Let the thread run until it finishes (max MAX_VISIBILITY_TIMEOUT), with a buffer of
                # BULK_DOWNLOAD_VISIBILITY_TIMEOUT
                while psql_process.is_alive() and (time.time() - overall_start_time) < MAX_VISIBILITY_TIMEOUT:
                    if message:
                        message.change_visibility(VisibilityTimeout=BULK_DOWNLOAD_VISIBILITY_TIMEOUT)
                    time.sleep(60)

                if (time.time() - overall_start_time) >= MAX_VISIBILITY_TIMEOUT:
                    # Process is running for longer than MAX_VISIBILITY_TIMEOUT, kill it
                    psql_process.terminate()

                    # Remove all temp files
                    os.close(temp_sql_file_fd)
                    os.remove(temp_sql_file_path)
                    shutil.rmtree(working_dir)
                    zipped_csvs.close()
                    os.remove(file_path)
                    raise TimeoutError('Bulk download job lasted longer than 4 hours')

                # save it to the zip
                zipped_csvs.write(split_csv_path, split_csv_name)
                os.close(temp_sql_file_fd)
                os.remove(temp_sql_file_path)
                logger.info('wrote {} took {} seconds'.format(split_csv_name, time.time() - start_split_writing))

                last_count = len(open(split_csv_path).readlines())
                if last_count < EXCEL_ROW_LIMIT + 1:
                    # Will be hit when line 201 ((split_csv - 1) * EXCEL_ROW_LIMIT) > number of rows in source
                    download_job.number_of_rows += EXCEL_ROW_LIMIT * (split_csv - 1) + last_count
                    reached_end = True
                else:
                    split_csv += 1

        shutil.rmtree(working_dir)
        zipped_csvs.close()
        download_job.file_size = os.stat(file_path).st_size

        if not settings.IS_LOCAL:
            bucket = settings.BULK_DOWNLOAD_S3_BUCKET_NAME
            region = settings.BULK_DOWNLOAD_AWS_REGION
            start_uploading = time.time()
            upload(bucket, region, file_path, os.path.basename(file_path), acl='public-read',
                   parallel_processes=multiprocessing.cpu_count())
            logger.info('uploading took {} seconds'.format(time.time() - start_uploading))
            os.remove(file_path)

    except Exception as e:
        # Set error message; job_status_id will be set in generate_bulk_zip.handle()
        download_job.error_message = 'An exception was raised while attempting to write the CSV:\n' + str(e)
        download_job.save()

        raise Exception(download_job.error_message)
    else:
        download_job.job_status_id = JOB_STATUS_DICT['finished']
        download_job.save()

    logger.info('Processed Job: {}\n'
                'Filename: {}\n'
                'Request Params: {}'.format(download_job.bulk_download_job_id,
                                            download_job.file_name,
                                            download_job.json_request))

    logger.info('generate_zips took {} seconds'.format(time.time() - start_zip_generation))

    return file_name


def execute_psql(temp_sql_file_path, split_csv_path):
    try:
        # Generate the csv with \copy
        cat_command = subprocess.Popen(['cat', temp_sql_file_path], stdout=subprocess.PIPE)
        subprocess.call(['psql', '-o', split_csv_path, os.environ['DATABASE_URL']], stdin=cat_command.stdout)
    except Exception as e:
        logger.error(str(e))
        raise e
