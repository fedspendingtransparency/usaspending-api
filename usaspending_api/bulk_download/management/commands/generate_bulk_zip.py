import logging
import csv
import json
from threading import Thread
import time

from django.core.management.base import BaseCommand
from django.conf import settings
from usaspending_api.bulk_download.models import BulkDownloadJob
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.common.csv_helpers import sqs_queue
from usaspending_api.bulk_download.filestreaming import csv_selection
from usaspending_api.bulk_download.v2.views import BulkDownloadAwardsViewSet

# Logging
# logging.basicConfig(filename='bulk-download-worker.log',
#                     format='%(levelname)s %(asctime)s :: %(message)s',
#                     datefmt='%m/%d/%Y %I:%M:%S %p',
#                     level=logging.INFO)
logger = logging.getLogger('console')

DEFAULT_BULK_DOWNLOAD_VISIBILITY_TIMEOUT = 60*30

# # AWS parameters
# BULK_DOWNLOAD_S3_BUCKET_NAME = os.environ.get('BULK_DOWNLOAD_S3_BUCKET_NAME')
# BULK_DOWNLOAD_SQS_QUEUE_NAME = os.environ.get('BULK_DOWNLOAD_SQS_QUEUE_NAME')
# BULK_DOWNLOAD_AWS_REGION = os.environ.get('BULK_DOWNLOAD_AWS_REGION')
# DATABASE_URL = os.environ.get('DATABASE_URL')


class Command(BaseCommand):

    current_job_id = None

    def get_current_job(self):
        """Return the job currently stored in current_job_id"""
        # the job_id is added to current_job_id at the beginning of the validate
        # route. we expect it to be here now, since validate is
        # currently the app's only functional route
        job_id = self.current_job_id
        if job_id:
            return BulkDownloadJob.objects.filter(bulk_download_job_id=job_id).first()


    def process_message(self, message, current_job):
        current_job.job_status_id = JOB_STATUS_DICT['running']
        current_job.save()
        # Recreate the sources
        json_request = json.loads(message.message_attributes['request']['StringValue'])
        csv_sources = BulkDownloadAwardsViewSet().get_csv_sources(json_request)
        kwargs = {
            'download_job': current_job,
            'file_name': message.message_attributes['file_name']['StringValue'],
            'columns': json.loads(message.message_attributes['columns']['StringValue']),
            'sources': csv_sources,
            'message': message
        }
        csv_selection.write_csvs(**kwargs)
        message.delete()


    def handle(self, *args, **options):
        """Run the application."""
        queue = sqs_queue(region_name=settings.BULK_DOWNLOAD_AWS_REGION,
                          QueueName=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME)

        logger.info('Starting SQS polling')
        while True:
            first_attempt = True
            try:
                # Grabs one (or more) messages from the queue
                messages = queue.receive_messages(WaitTimeSeconds=10, MessageAttributeNames=['All'],
                                                  VisibilityTimeout=DEFAULT_BULK_DOWNLOAD_VISIBILITY_TIMEOUT)
                for message in messages:
                    logger.info('Message Received: {}'.format(message))
                    if message.message_attributes is not None:
                        # Retrieve the job from the message
                        self.current_job_id = message.message_attributes['download_job_id']['StringValue']
                        current_job = self.get_current_job()

                        first_attempt = current_job.error_message is None
                        self.process_message(message, current_job)
            except Exception as e:
                # Handle uncaught exceptions in validation process.
                job.error_message = str(e)
                job.save()

                logger.error(job.error_message)

                job = self.get_current_job()
                if job:
                    job.job_status_id = JOB_STATUS_DICT['ready' if first_attempt else 'failed']
                    job.save()
            finally:
                # Set visibility to 0 so that another attempt can be made to process in SQS immediately,
                # instead of waiting for the timeout window to expire
                for message in messages:
                    try:
                        message.change_visibility(VisibilityTimeout=0)
                    except Exception:
                        # TODO: check existence instead of catching error
                        logger.error('Failed to change visibility on message')
