import csv
import io
import logging
import itertools
import json
import jsonpickle

import boto
import smart_open
import zipstream
from django.conf import settings
from django.core import serializers

from usaspending_api.bulk_download.lookups import JOB_STATUS_DICT
from usaspending_api.bulk_download.v2 import download_column_historical_lookups

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
        # yield headers
        query_paths = [self.query_paths[hn] for hn in headers]
        yield from self.queryset.values_list(*query_paths).iterator()

    def toJsonDict(self):
        json_dict = {
            "model_type": self.model_type,
            "file_type": self.file_type,
            "query": jsonpickle.dumps(self.queryset.query) if self.queryset is not None else None
        }
        return json_dict


def calculate_number_of_csvs(queryset, limit):
    csvs = 0
    reached_limit = False

    while not reached_limit:
        csvs += 1
        try:
            queryset[limit*csvs]
        except IndexError:
            reached_limit = True
    return csvs


def write_csvs(download_job, file_name, columns, sources):
    """Derive the relevant location and write CSVs to it.

    :return: the final file name (complete with prefix)"""

    download_job.job_status_id = JOB_STATUS_DICT['running']
    download_job.number_of_rows = 0
    download_job.file_size = 0
    download_job.save()

    try:
        file_path = settings.CSV_LOCAL_PATH + file_name
        zstream = zipstream.ZipFile(allowZip64=True)

        logger.debug('Generating {}'.format(file_name))

        source_map = {"contracts": sources[0],
                      "assistance": sources[1]}

        for source_name, source in source_map.items():
            source_limit = (source.queryset.count() // EXCEL_ROW_LIMIT) + 1
            source_iterator = list(source.row_emitter(columns))
            headers = source.columns(columns)
            logger.debug(source.queryset.query)
            for split_csv in range(1, source_limit+1):
                split_csv_name = '{}_{}.csv'.format(source_name, split_csv)
                end_row = split_csv*EXCEL_ROW_LIMIT if split_csv != source_limit else None
                split_source_iterator = itertools.islice(iter(source_iterator), (split_csv-1)*EXCEL_ROW_LIMIT, end_row)
                split_source_iterator = itertools.chain([headers], split_source_iterator)

                zstream.write_iter(split_csv_name, csv_row_emitter(split_source_iterator, download_job))
            logger.debug('wrote {}.csv'.format(source_name))

        if settings.IS_LOCAL:

            with open(file_path, 'wb') as zipfile:
                for chunk in zstream:
                    zipfile.write(chunk)

                download_job.file_size = zipfile.tell()
        else:
            bucket = settings.BULK_DOWNLOAD_S3_BUCKET_NAME
            region = settings.BULK_DOWNLOAD_AWS_REGION
            s3_bucket = boto.s3.connect_to_region(region).get_bucket(bucket)
            conn = s3_bucket.new_key(file_name)
            stream = smart_open.smart_open(
                conn, 'w', min_part_size=BUFFER_SIZE)
            for chunk in zstream:
                stream.write(chunk)
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
