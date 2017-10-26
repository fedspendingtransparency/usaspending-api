import csv
import io
import logging
import time

import boto
import smart_open
import zipstream
from django.conf import settings

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.v2 import download_column_historical_lookups

BUFFER_SIZE = (5 * 1024 ** 2)

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
            if download_job.number_of_rows > settings.MAX_DOWNLOAD_LIMIT:
                break
                raise Exception('Requested query beyond max supported ({})'.format(settings.MAX_DOWNLOAD_LIMIT))

        yield string_buffer.getvalue().encode('utf8')


class CsvSource:
    def __init__(self, model_type, file_type):
        self.model_type = model_type
        self.file_type = file_type
        self.human_names = download_column_historical_lookups.human_names[
            model_type][file_type]
        self.query_paths = download_column_historical_lookups.query_paths[
            model_type][file_type]

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
        yield headers
        query_paths = [self.query_paths[hn] for hn in headers]
        yield from self.queryset.values_list(*query_paths).iterator()


def write_csvs(download_job, file_name, columns, sources):
    """Derive the relevant location and write CSVs to it.

    :return: the final file name (complete with prefix)"""

    download_job.job_status_id = JOB_STATUS_DICT['running']
    download_job.number_of_rows = 0
    download_job.file_size = 0
    download_job.save()

    try:
        file_path = settings.CSV_LOCAL_PATH + file_name
        zstream = zipstream.ZipFile()
        minutes = settings.DOWNLOAD_TIMEOUT_MIN_LIMIT
        timeout = time.time() + 60 * minutes

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
                    # Adding timeout to break the stream if exceeding time limit, closes out thread
                    if time.time() > timeout:
                        raise Exception('Stream exceeded time of {} minutes.'.format(minutes))

                download_job.file_size = zipfile.tell()
        else:
            bucket = settings.CSV_S3_BUCKET_NAME
            region = settings.CSV_AWS_REGION
            s3_bucket = boto.s3.connect_to_region(region).get_bucket(bucket)
            conn = s3_bucket.new_key(file_name)
            stream = smart_open.smart_open(
                conn, 'w', min_part_size=BUFFER_SIZE)
            for chunk in zstream:
                stream.write(chunk)
                # Adding timeout to break the stream if exceeding time limit, closes out thread
                if time.time() > timeout:
                    raise Exception('Stream exceeded time of {} minutes.'.format(minutes))
            download_job.file_size = stream.total_size

    except Exception as e:
        download_job.job_status_id = JOB_STATUS_DICT['failed']
        download_job.error_message = 'An exception was raised while attempting to write the CSV'
        if settings.DEBUG:
            download_job.error_message += '\n' + str(e)
    else:
        download_job.job_status_id = JOB_STATUS_DICT['finished']
    finally:
        try:
            stream.close()
            s3_bucket.lookup(conn.key).set_acl('public-read')
        except NameError:
            # there was no stream to close
            pass

    download_job.save()

    return file_name
