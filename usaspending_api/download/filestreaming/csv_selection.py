import logging
import os

from django.conf import settings

from usaspending_api.download.lookups import JOB_STATUS_DICT
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


def write_csv_from_querysets(download_job, file_name, upload_name, columns,
                             querysets):
    """Derive the relevant location and write a CSV to it.

    Given a list of querysets, mashes them horizontally into one CSV

    :return: the final file name (complete with prefix)"""

    offset = 0
    header = []
    offsets = [
        0,
    ]  # `None`s to insert before each result set for horizontal spacing
    widths = []  # Width of each result set
    all_field_names = []
    querysets_needed = []

    # TODO: the linked transaction table
    # TODO: how do we detect errors in the thread?

    for queryset in querysets:
        if queryset.first():
            field_names = [
                q.name for q in queryset.model._meta.fields
                if q.name in queryset.values()[0].keys()
            ]
            if columns:
                field_names = [f for f in field_names if f in columns]
                if not field_names:
                    continue  # no columns requested, omit this queryset entirely
            querysets_needed.append(queryset)
            download_job.number_of_rows = (download_job.number_of_rows or 0
                                           ) + queryset.count()
            widths.append(len(field_names))
            header.extend(field_names)
            offsets.append(offset + len(field_names))
            offset += len(field_names)
            all_field_names.append(field_names)

    download_job.number_of_columns = len(header)
    download_job.save()

    missing = set(columns) - set(header)
    if missing:
        raise InvalidParameterException('Columns not available: {}'.format(
            missing))
        # TODO: If column exists, but only in tables which had no results

    def row_emitter():
        for (idx, queryset) in enumerate(querysets_needed):
            leading_empty = [None, ] * offsets[idx]
            trailing_empty = [None, ] * (
                len(header) - offsets[idx] - widths[idx])
            for row in queryset.values():
                # Make a list of values in the same order as in the headers
                values = [row.get(f) for f in all_field_names[idx]]
                yield leading_empty + values + trailing_empty

    return write_csv(
        download_job,
        file_name,
        upload_name,
        header=header,
        body=row_emitter())
