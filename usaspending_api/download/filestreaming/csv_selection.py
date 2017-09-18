import csv
import io
import logging
import os

import boto
import smart_open
import zipstream
from django.conf import settings

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.v2 import download_column_lookups

BUFFER_SIZE = (5 * 1024**2)
BATCH_SIZE = 100

logger = logging.getLogger(__name__)


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
    def __init__(self, queryset, model_type, file_type):
        self.queryset = queryset
        self.human_names = download_column_lookups.human_names[model_type][
            file_type]
        self.query_paths = download_column_lookups.query_paths[model_type][
            file_type]

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
                elif column in self.human_names.inv:  # given query path, get human name
                    headers.append(self.human_names.inv[column])
        else:
            result = self.human_names

        # remove headers that we don't have a query path for
        result = [h for h in result if h in self.query_paths]

        return result

    def row_emitter(self, headers_requested):
        headers = self.columns(headers_requested)
        yield headers
        query_paths = [self.query_paths[hn] for hn in headers]
        yield from self.queryset.values_list(*query_paths).iterator()


def write_csvs(download_job, file_name, upload_name, columns, sources):
    """Derive the relevant location and write a CSV to it.

    :return: the final file name (complete with prefix)"""

    # TODO: raise InvalidParameterException('{} not a known field name'.format(column))

    download_job.job_status_id = JOB_STATUS_DICT['running']
    download_job.number_of_rows = 0
    download_job.file_size = 0
    download_job.save()

    try:
        file_path = settings.CSV_LOCAL_PATH + file_name
        zstream = zipstream.ZipFile()
        logger.debug('Generating {}'.format(file_name))
        zstream.write_iter('contracts.csv',
                           csv_row_emitter(sources[0].row_emitter(columns),
                                           download_job))
        logger.debug('wrote contracts.csv')
        zstream.write_iter('assistance.csv',
                           csv_row_emitter(sources[1].row_emitter(columns),
                                           download_job))
        logger.debug('wrote assistance.csv')

        if settings.IS_LOCAL:
            with open(file_path, 'wb') as zipfile:
                for chunk in zstream:
                    zipfile.write(chunk)
                download_job.file_size = zipfile.tell()
        else:
            bucket = settings.CSV_S3_BUCKET_NAME
            region = settings.CSV_AWS_REGION
            s3_bucket = boto.s3.connect_to_region(region).get_bucket(bucket)
            conn = s3_bucket.new_key(file_name)
            stream = smart_open.smart_open(conn, 'w', min_part_size=BUFFER_SIZE)
            for chunk in zstream:
                stream.write(chunk)
            download_job.file_size = stream.total_size

    except Exception as e:
        # TODO: Add proper exception logging
        download_job.job_status_id = JOB_STATUS_DICT['failed']
        download_job.error_message = 'An exception was raised while attempting to write the CSV'
        if settings.DEBUG:
            download_job.error_message += '\n' + str(e)
    else:
        download_job.job_status_id = JOB_STATUS_DICT['finished']

    download_job.save()

    return file_name
