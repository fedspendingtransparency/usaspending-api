import logging
import os

from django.conf import settings

from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.v2 import download_column_lookups
from usaspending_api.download.filestreaming.csv_local_writer import CsvLocalWriter
from usaspending_api.download.filestreaming.csv_s3_writer import CsvS3Writer
from usaspending_api.download.filestreaming.s3_handler import S3Handler
from usaspending_api.common.exceptions import InvalidParameterException

logger = logging.getLogger(__name__)


def write_csv(download_job, file_name, upload_name, header, body):
    """Derive the relevant location and write a CSV to it.
    :return: the final file name (complete with prefix)"""

    s3_handler = S3Handler()

    download_job.job_status_id = JOB_STATUS_DICT['running']
    download_job.number_of_rows = 0
    download_job.save()

    try:
        if settings.IS_LOCAL:
            file_name = settings.CSV_LOCAL_PATH + file_name + '.csv'
            csv_writer = CsvLocalWriter(file_name, header)
            message = 'Writing file locally...'
        else:
            bucket = settings.CSV_S3_BUCKET_NAME
            region = settings.CSV_AWS_REGION
            csv_writer = CsvS3Writer(region, bucket, upload_name, header)
            message = 'Writing file to S3...'

        logger.debug(message)

        with csv_writer as writer:
            for line in body:
                writer.write(line)
                download_job.number_of_rows += 1
            writer.finish_batch()

    except:
        # TODO: Add proper exception logging
        download_job.job_status_id = JOB_STATUS_DICT['failed']
        download_job.error_message = 'An exception was raised while attempting to write the CSV'
    else:
        download_job.number_of_columns = len(header)
        download_job.file_size = os.path.getsize(file_name) if settings.IS_LOCAL else \
            s3_handler.get_file_size(file_name)
        download_job.job_status_id = JOB_STATUS_DICT['finished']

    download_job.save()


class CsvSource:

    def __init__(self, queryset, model_type, file_type):
        self.queryset = queryset
        self.human_names = download_column_lookups.human_names[model_type][file_type]
        self.query_paths = download_column_lookups.query_paths[model_type][file_type]

    def values(self, header):
        query_paths = [self.query_paths[hn] for hn in header]
        return self.queryset.values_list(query_paths).iterator()


def write_csv_from_querysets(download_job, file_name, upload_name, columns,
                             sources):
    """Derive the relevant location and write a CSV to it.

    Given a list of querysets, mashes them horizontally into one CSV

    :return: the final file name (complete with prefix)"""

    source = sources[0]  # TODO: do this for both, then zip
    if columns:
        headers = []
        for column in columns:
            if column in source.human_names:
                headers.append(column)
            elif column in source.human_names.inv:  # given query path, get human name
                headers.append(source.human_names.inv[column])
            else:
                raise InvalidParameterException('{} not a known field name'.format(column))
    else:
        headers = source.human_names

    # remove headers that we don't have a query path for
    headers = [h for h in headers if h in source.query_paths]

    def row_emitter():
        query_paths = [source.query_paths[hn] for hn in headers]
        yield from source.queryset.values_list(*query_paths).iterator()

    return write_csv(
        download_job,
        file_name,
        upload_name,
        header=headers,
        body=row_emitter())
