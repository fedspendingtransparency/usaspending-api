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
            file_name = settings.CSV_LOCAL_PATH + file_name
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

    def __init__(self, queryset):
        self.queryset = queryset

    def values_from_db_col_names(self):
        """Yields list of values from queryset, interspersed with None

        `db_col_names` is a list of `None`s and column names.
        Each row yields either None or the

        >>> list(values_from_db_col_names(qry, ['colA', None, 'colB']))
        [[valA1, None, valB1],
         [valA2, None, valB2]]
        """
        col_names = [c for c in self.db_col_names if c]
        rows = self.queryset.values_list(*col_names)
        for row in rows:
            yield [(row[i] if c else None) for (i, c) in enumerate(self.db_col_names)]

# possible renamings
# col_names -> query_paths

class TransactionContractCsvSource(CsvSource):

    all_column_list = download_column_lookups.transaction_columns
    column_name_lookups = download_column_lookups.transaction_d1_columns


class TransactionAssistanceCsvSource(CsvSource):

    all_column_list = download_column_lookups.transaction_columns
    column_name_lookups = download_column_lookups.transaction_d2_columns


class AwardCsvSource(CsvSource):

    all_column_list = download_column_lookups.award_columns
    column_name_lookups = download_column_lookups.award_column_map


def write_csv_from_querysets(download_job, file_name, upload_name, columns,
                             sources):
    """Derive the relevant location and write a CSV to it.

    Given a list of querysets, mashes them horizontally into one CSV

    :return: the final file name (complete with prefix)"""

    headers = columns or sources[0].all_column_list
    unfound = set(headers)

    for source in sources:
        source.db_col_names = []
        for (i, header) in enumerate(headers[:]):
            db_col_name = source.column_name_lookups.get(header)
            if db_col_name:  # Col requested with downloadable name
                source.db_col_names.append(db_col_name)
                unfound -= set([header, ])
            else:
                full_name = source.column_name_lookups.inv.get(header)
                if full_name:  # Col requested with db-side name
                    source.db_col_names.append(header)
                    unfound -= set([header, ])
                    headers[i] = full_name

    if unfound:
        raise InvalidParameterException('Columns {} not available'.format(unfound))

    def row_emitter():
        for source in sources:
            yield from source.values_from_db_col_names()

    return write_csv(
        download_job,
        file_name,
        upload_name,
        header=headers,
        body=row_emitter())
