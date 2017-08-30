import logging
import os

from django.conf import settings

from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.filestreaming.csv_local_writer import CsvLocalWriter
from usaspending_api.download.filestreaming.csv_s3_writer import CsvS3Writer
from usaspending_api.download.filestreaming.s3_handler import S3Handler


logger = logging.getLogger(__name__)


def write_csv(download_job, file_name, upload_name, header, body):
    """Derive the relevant location and write a CSV to it.
    :return: the final file name (complete with prefix)"""

    s3_handler = S3Handler()

    download_job.job_status_id = JOB_STATUS_DICT['running']
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
            writer.finish_batch()

    except:
        # TODO: Add proper exception logging
        download_job.job_status_id = JOB_STATUS_DICT['failed']
        download_job.error_message = 'An exception was raised while attempting to write the CSV'
    else:
        download_job.number_of_columns = len(header)
        download_job.number_of_rows = len(body)
        download_job.file_size = os.path.getsize(file_name) if settings.IS_LOCAL else \
            s3_handler.get_file_size(file_name)
        download_job.job_status_id = JOB_STATUS_DICT['finished']

    download_job.save()
