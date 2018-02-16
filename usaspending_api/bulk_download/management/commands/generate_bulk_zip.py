import logging
import json
import botocore

from django.core.management.base import BaseCommand
from django.conf import settings
from usaspending_api.bulk_download.models import BulkDownloadJob
from usaspending_api.download.v2.views import YearConstraintDownloadViewSet
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.common.csv_helpers import sqs_queue
from usaspending_api.bulk_download.filestreaming import csv_selection

logger = logging.getLogger('console')

DEFAULT_BULK_DOWNLOAD_VISIBILITY_TIMEOUT = 60*30


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
                                                  VisibilityTimeout=DEFAULT_BULK_DOWNLOAD_VISIBILITY_TIMEOUT)
                for message in messages:
                    logger.info('Message Received: {}'.format(message))
                    if message.message_attributes is not None:
                        # Retrieve the job from the message and update it
                        job_id = message.message_attributes['download_job_id']['StringValue']
                        current_job = BulkDownloadJob.objects.filter(bulk_download_job_id=job_id).first()
                        second_attempt = current_job.error_message is not None
                        current_job.job_status_id = JOB_STATUS_DICT['running']
                        current_job.save()

                        # Recreate the sources
                        json_request = json.loads(message.message_attributes['request']['StringValue'])
                        csv_sources = YearConstraintDownloadViewSet().get_csv_sources(json_request)

                        # Begin writing the CSVs
                        kwargs = {
                            'download_job': current_job,
                            'file_name': message.message_attributes['file_name']['StringValue'],
                            'columns': json.loads(message.message_attributes['columns']['StringValue']),
                            'sources': csv_sources,
                            'message': message
                        }
                        csv_selection.write_csvs(**kwargs)

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
                # Set visibility to 0 so that another attempt can be made to process in SQS immediately,
                # instead of waiting for the timeout window to expire
                for message in messages:
                    try:
                        message.change_visibility(VisibilityTimeout=0)
                    except botocore.exceptions.ClientError:
                        # TODO: check existence instead of catching error
                        continue
