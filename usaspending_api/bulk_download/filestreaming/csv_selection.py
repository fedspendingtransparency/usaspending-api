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

import boto
from django.conf import settings

from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.v2 import download_column_historical_lookups

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
    def __init__(self, model_type, file_type):
        self.model_type = model_type
        self.file_type = file_type
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
            'query': jsonpickle.dumps(self.queryset.query) if self.queryset is not None else None
        }
        return json_dict


def calculate_number_of_csvs(queryset, limit):
    # This could also be done with the commented line below but this performed better
    # return (queryset.count() // limit) + 1
    csvs = 0
    reached_limit = False
    while not reached_limit:
        csvs += 1
        try:
            queryset[limit * csvs]
        except IndexError:
            reached_limit = True
    return csvs


def date_query_fix(query):
    """Adds quotes around dates to execute the query"""
    for date_string in re.findall('\d{4}-\d{2}-\d{2}', query):
        query = query.replace(date_string, '\'{}\''.format(date_string))
    return query


def in_list_query_fix(query):
    """Adds quotes around dates to execute the query"""
    for in_list_string in re.findall('IN \([^\)]*\)', query):
        quoted_strings = ",".join('\'{}\''.format(str_value.strip())
                                  for str_value in in_list_string[4:-1].strip().split(','))
        query = query.replace(in_list_string, 'IN ({})'.format(quoted_strings))
    return query


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


def write_csvs(download_job, file_name, columns, sources):
    """Derive the relevant location and write CSVs to it.

    :return: the final file name (complete with prefix)"""
    start_zip_generation = time.time()

    download_job.job_status_id = JOB_STATUS_DICT['running']
    download_job.number_of_rows = 0
    download_job.number_of_columns = 0
    download_job.file_size = 0
    download_job.save()

    try:
        file_path = settings.BULK_DOWNLOAD_LOCAL_PATH + file_name
        working_dir = os.path.splitext(file_path)[0]
        if not os.path.exists(working_dir):
            os.mkdir(working_dir)
        zipped_csvs = zipfile.ZipFile(file_path, 'w', allowZip64=True)

        logger.info('Generating {}'.format(file_name))

        source_map = {'contracts': sources[0],
                      'assistance': sources[1]}

        for source_name, source in source_map.items():
            # Figure out how many csvs we need
            start_count = time.time()
            source_limit = calculate_number_of_csvs(source.queryset, EXCEL_ROW_LIMIT)
            logger.info('source_limit({}) took {} seconds'.format(source_limit, time.time() - start_count))

            source_query = source.row_emitter(columns)
            download_job.number_of_columns = max(download_job.number_of_columns, len(source.columns(columns)))
            start_writing = time.time()
            for split_csv in range(1, source_limit + 1):
                split_csv_name = '{}_{}.csv'.format(source_name, split_csv)
                split_csv_path = os.path.join(working_dir, split_csv_name)
                # Generate the final query, values, limits, dates fixed
                split_csv_query = source_query[(split_csv - 1) * EXCEL_ROW_LIMIT:split_csv * EXCEL_ROW_LIMIT]
                split_csv_query_raw = in_list_query_fix(date_query_fix(str(split_csv_query.query)))
                # Generate the csv with \copy
                psql_command = subprocess.Popen(
                    ['echo', '\copy ({}) To STDOUT with CSV HEADER'.format(split_csv_query_raw)],
                    stdout=subprocess.PIPE
                )
                subprocess.call(['psql', '-o', split_csv_path, os.environ['DATABASE_URL']], stdin=psql_command.stdout)
                # save it to the zip
                zipped_csvs.write(split_csv_path, split_csv_name)
                if split_csv == source_limit:
                    # could've called .count() earlier on the original query but we know how many millions
                    # it is, so let's just get a count of the remaining rows to save time
                    download_job.number_of_rows += EXCEL_ROW_LIMIT * (split_csv - 1) + split_csv_query.count()
            logger.info('wrote {}.csv took {} seconds'.format(source_name, time.time() - start_writing))

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
        download_job.job_status_id = JOB_STATUS_DICT['failed']
        download_job.error_message = 'An exception was raised while attempting to write the CSV'
        if settings.DEBUG:
            download_job.error_message += '\n' + str(e)
    else:
        download_job.job_status_id = JOB_STATUS_DICT['finished']

    download_job.save()

    logger.info('generate_zips took {} seconds'.format(time.time() - start_zip_generation))

    return file_name
