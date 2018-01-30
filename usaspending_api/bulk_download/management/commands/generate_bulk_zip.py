import logging
import csv
import json
import jsonpickle

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

# Average time for Bulk Download's an hour so far but theoretically someone could request
# all data from all agencies from all fiscal years, max is 12 hours, setting this to 3
BULK_DOWNLOAD_VISIBILITY_TIMEOUT = 10800

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

    def mark_job_status(self, job_id, status_name, skip_check=False):
        """
        Mark job as having specified status.
        Jobs being marked as finished will add dependent jobs to queue.

        Args:
            job_id: ID for job being marked
            status_name: Status to change job to
        """
        job = BulkDownloadJob.objects.filter(bulk_download_job_id=job_id).first()
        # update job status
        job.job_status_id = JOB_STATUS_DICT[status_name]

    def handle(self, *args, **options):
        """Run the application."""

        queue = sqs_queue(region_name=settings.BULK_DOWNLOAD_AWS_REGION,
                          QueueName=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME)

        logger.info('Starting SQS polling')
        while True:
            processed_messages = []
            try:
                # Grabs one (or more) messages from the queue
                messages = queue.receive_messages(WaitTimeSeconds=10,
                                                  VisibilityTimeout=BULK_DOWNLOAD_VISIBILITY_TIMEOUT,
                                                  MessageAttributeNames=['All'])
                for message in messages:
                    logger.info('Message Received: {}'.format(message))
                    if message.message_attributes is not None:
                        self.current_job_id = message.message_attributes['download_job_id']['StringValue']
                        sources = []

                        # Recreate the sources
                        json_request = json.loads(message.message_attributes['request']['StringValue'])
                        csv_sources = BulkDownloadAwardsViewSet().get_csv_sources(json_request)
                        kwargs = {
                            'download_job': self.get_current_job(),
                            'file_name': message.message_attributes['file_name']['StringValue'],
                            'columns': json.loads(message.message_attributes['columns']['StringValue']),
                            'sources': csv_sources
                        }
                        csv_selection.write_csvs(**kwargs)

                    # delete from SQS once processed
                    message.delete()
                    processed_messages.append(message)
            except Exception as e:
                # Handle uncaught exceptions in validation process.
                logger.error(str(e))

                # csv-specific errors get a different job status and response code
                if isinstance(e, ValueError) or isinstance(e, csv.Error) or isinstance(e, UnicodeDecodeError):
                    job_status = 'invalid'
                else:
                    job_status = 'failed'
                job = self.get_current_job()
                if job:
                    self.mark_job_status(job.bulk_download_job_id, job_status)
            finally:
                # Set visibility to 0 so that another attempt can be made to process in SQS immediately,
                # instead of waiting for the timeout window to expire
                for message in messages:
                    if message not in processed_messages:
                        message.change_visibility(VisibilityTimeout=0)
