import botocore
import logging

from django.core.management.base import BaseCommand
from django.conf import settings

from usaspending_api.common.csv_helpers import sqs_queue
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.models import DownloadJob
from usaspending_api.download.filestreaming import csv_generation

logger = logging.getLogger('console')

DEFAULT_VISIBILITY_TIMEOUT = 60*30


class Command(BaseCommand):

    def handle(self, *args, **options):
        """Run the application."""
        queue = sqs_queue(region_name=settings.BULK_DOWNLOAD_AWS_REGION,
                          QueueName=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME)

        logger.info('Starting SQS polling')
        while True:
            second_attempt = True
            try:
                # Grabs one (or more) messages from the queue
                messages = queue.receive_messages(WaitTimeSeconds=10, MessageAttributeNames=['All'],
                                                  VisibilityTimeout=DEFAULT_VISIBILITY_TIMEOUT)
                for message in messages:
                    logger.info('Message Received: {}'.format(message))
                    if message.body is not None:
                        # Retrieve and update the job
                        current_job = DownloadJob.objects.filter(download_job_id=int(message.body)).first()
                        second_attempt = current_job.error_message is not None

                        # Begin writing the CSVs
                        csv_generation.generate_csvs(download_job=current_job, sqs_message=message)

                        # If successful, we do not want to run again; delete
                        message.delete()
            except Exception as e:
                # Handle uncaught exceptions in validation process
                logger.error(str(e))

                if current_job:
                    current_job.error_message = str(e)
                    current_job.job_status_id = JOB_STATUS_DICT['failed' if second_attempt else 'ready']
                    current_job.save()
            finally:
                # Set visibility to 0 so that another attempt can be made to process in SQS immediately, instead of
                # waiting for the timeout window to expire
                for message in messages:
                    try:
                        message.change_visibility(VisibilityTimeout=0)
                    except botocore.exceptions.ClientError:
                        # TODO: check existence instead of catching error
                        continue
