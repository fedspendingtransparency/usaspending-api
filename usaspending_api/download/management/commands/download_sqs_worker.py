import botocore
import logging
import signal
import os

from multiprocessing import Process, Queue, Value
from django.conf import settings
from django.core.management.base import BaseCommand

from usaspending_api.common.sqs_helpers import get_sqs_queue_resource
from usaspending_api.download.download_exceptions import FatalError
from usaspending_api.download.filestreaming import csv_generation
from usaspending_api.download.helpers import write_to_download_log as write_to_log
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.models import DownloadJob

DEFAULT_VISIBILITY_TIMEOUT = 60 * 30

logger = logging.getLogger("console")


class Command(BaseCommand):
    def handle(self, *args, **options):
        # signal.signal(signal.SIGINT, halt_command_handler)  # route signal handling to custom function
        download_service_app()
        print("Bye")


def halt_command_handler(signum, frame):
    print("entered halt_command_handler()")
    raise SystemExit


def download_service_app():
    write_to_log(message="Starting SQS polling")
    while True:
        print("pid: {}".format(os.getpid()))
        second_attempt = True
        try:
            sqs_message = poll_sqs_for_message(settings.BULK_DOWNLOAD_SQS_QUEUE_NAME, 3)

            if sqs_message:
                write_to_log(message="Message Received: {}".format(sqs_message))
                if sqs_message.body is not None:
                    # Retrieve and update the job
                    download_job = DownloadJob.objects.filter(download_job_id=int(sqs_message.body)).first()
                    if download_job is None:
                        raise FatalError("Download Job {} Missing in DB!!!".format(sqs_message.body))
                    second_attempt = download_job.error_message is not None

                    # Retrieve the data and write to the CSV(s)
                    csv_generation.generate_csvs(download_job=download_job, sqs_message=sqs_message)

                    # If successful, remove message from queue
                    if remove_message_from_sqs(sqs_message):
                        sqs_message = None

        except KeyboardInterrupt as e:
            write_to_log(message="Process received external SIGNAL to terminate.", download_job=download_job)

            if download_job:
                download_job.error_message = None
                download_job.job_status_id = JOB_STATUS_DICT["killed"]
                download_job.save()
            break

        except FatalError as e:
            write_to_log(message=str(e), download_job=download_job, is_error=True)
            raise SystemExit(1)

        except Exception as e:
            logger.error(e)
            write_to_log(message=str(e), download_job=download_job, is_error=True)

            if download_job:
                download_job.error_message = str(e)
                download_job.job_status_id = JOB_STATUS_DICT["failed" if second_attempt else "ready"]
                download_job.save()
        finally:
            # Set visibility to 0 so that another attempt can be made to process in SQS immediately, instead of
            # waiting for the timeout period to expire
            if sqs_message:
                set_sqs_message_visibility(sqs_message, 0)
                write_to_log(message="Job released to queue", download_job=download_job)


def poll_sqs_for_message(queue_name, wait_time=10):
    """ Returns 0 or 1 message from the queue"""
    try:
        queue = get_sqs_queue_resource(queue_name=queue_name)
        sqs_message = queue.receive_messages(
            WaitTimeSeconds=wait_time,
            MessageAttributeNames=["All"],
            VisibilityTimeout=DEFAULT_VISIBILITY_TIMEOUT,
            MaxNumberOfMessages=1
        )
    except botocore.exceptions.ClientError:
        logger.exception("SQS connection issue. Investigate")
        raise SystemExit(1)

    return sqs_message[0] if sqs_message else None


def remove_message_from_sqs(message):
    try:
        message.delete()
    except botocore.exceptions.ClientError:
        logger.exception("Unable to delete SQS message. Investigate")
        raise SystemExit(1)


def set_sqs_message_visibility(message, new_visibility):
    try:
        message.change_visibility(VisibilityTimeout=new_visibility)
        # TODO: check existence instead of catching error
    except botocore.exceptions.ClientError as e:
        logger.exception()
        raise FatalError(e)


def process_guarddog(process_list):
    """
        pass in a list of multiprocess Process objects.
        If one errored then terminate the others and return True
    """
    for proc in process_list:
        # If exitcode is None, process is still running. exit code 0 is normal
        if proc.exitcode not in (None, 0):
            msg = 'TERMINATING ALL PROCESSES AND QUITTING!!! ' + \
                '{} exited with error. Returned {}'.format(proc.name, proc.exitcode)
            logger.info(msg)
            [x.terminate() for x in process_list]
            return True
    return False


# process_list.append(Process(
#             name='Download Proccess',
#             target=download_db_records,
#             args=(download_queue, es_ingest_queue, self.config)))


# while True:
#             sleep(10)
#             if process_guarddog(process_list):
#                 raise SystemExit(1)
#             elif all([not x.is_alive() for x in process_list]):
#                 printf({'msg': 'All ETL processes completed execution with no error codes'})
#                 break
