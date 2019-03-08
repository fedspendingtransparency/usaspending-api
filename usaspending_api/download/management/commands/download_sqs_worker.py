import botocore
import functools
import logging
import os
import signal
import time

from multiprocessing import Process, Value

from django.conf import settings
from django.core.management.base import BaseCommand

from usaspending_api.common.sqs_helpers import get_sqs_queue_resource
from usaspending_api.download.download_exceptions import FatalError
from usaspending_api.download.filestreaming import csv_generation
from usaspending_api.download.helpers import write_to_download_log as write_to_log
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.models import DownloadJob

CURRENT_DOWNLOAD_JOB = Value()
CURRENT_MESSAGE = Value()
DEFAULT_VISIBILITY_TIMEOUT = 60 * 30
PROCESS_FAIL_COUNT = 0
PROCESS_FAIL_MAX = 3
PROCESS_RESTART_DELAY = 15

logger = logging.getLogger("console")


class Command(BaseCommand):
    def handle(self, *args, **options):
        # route signal handling to custom function
        for sig in [signal.SIGINT, signal.SIGQUIT, signal.SIGTERM]:
            signal.signal(sig, signal_handler)

        download_service_app()
        logger.info("Bye")


def signal_handler(signum, frame):
    print("entered signal_handler({})".format(signum))
    # import pdb; pdb.set_trace()
    raise SystemExit


def download_service_manager():
    download_app = create_and_start_new_process()

    global PROCESS_FAIL_COUNT
    global PROCESS_FAIL_MAX

    while True:
        time.sleep(PROCESS_RESTART_DELAY / 2)
        if download_app.exitcode not in (None, 0):
            # Download job terminated with an error
            raise SystemExit(1)
        elif not download_app.is_alive():
            # Download job completed with no error status codes (this is unexpected)
            msg_prefix = "Download Service App unexpectedly ended with no errors."

            if PROCESS_FAIL_COUNT < PROCESS_FAIL_MAX:
                PROCESS_FAIL_COUNT += 1
                write_to_log(
                    message="{} Creating new process in {}s".format(msg_prefix, PROCESS_RESTART_DELAY),
                    download_job=CURRENT_DOWNLOAD_JOB,
                )
                time.sleep(PROCESS_RESTART_DELAY)
                download_app = create_and_start_new_process()
            else:
                write_to_log(
                    message="{} Exceeded {} retries. Check logs".format(msg_prefix, PROCESS_FAIL_MAX),
                    download_job=CURRENT_DOWNLOAD_JOB,
                    is_error=True,
                )
                raise SystemExit(1)


def create_and_start_new_process():
    download_app = Process(
        name="Download Service Worker Proccess",
        target=download_service_app,
        args=(CURRENT_DOWNLOAD_JOB, CURRENT_MESSAGE),
    )
    download_app.start()

    return download_app


def error_wrapper(func):
    @functools.wraps(func)
    def wrap_for_errors(*args, **kwargs):
        try:
            download_job = kwargs.get('download_job') or args[0]
            sqs_message = kwargs.get('sqs_message') or args[1]
            value = func(*args, **kwargs)
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
                download_job.job_status_id = JOB_STATUS_DICT["failed"]
                download_job.save()
        finally:
            # Set visibility to 0 so that another attempt can be made to process in SQS immediately, instead of
            # waiting for the timeout period to expire
            if sqs_message:
                set_sqs_message_visibility(sqs_message, 0)
                write_to_log(message="Job released to queue", download_job=download_job)

        return value
    return wrap_for_errors


@error_wrapper
def download_service_app(download_job=None, sqs_message=None):
    write_to_log("Started new Download Service App with pid {}".format(os.getpid()))
    while True:
        # second_attempt = True
        sqs_message = poll_sqs_for_message(settings.BULK_DOWNLOAD_SQS_QUEUE_NAME, 3)

        if not sqs_message:
            continue

        write_to_log(message="Message Received: {}".format(sqs_message))
        if sqs_message.body is not None:
            # Retrieve and update the job
            download_job = DownloadJob.objects.filter(download_job_id=int(sqs_message.body)).first()
            if download_job is None:
                raise FatalError("Download Job {} Missing in DB!!!".format(sqs_message.body))

            # Retrieve the data and write to the CSV(s)
            csv_generation.generate_csvs(download_job=download_job, sqs_message=sqs_message)

            # If successful, remove message from queue
            if remove_message_from_sqs(sqs_message):
                sqs_message = None


def poll_sqs_for_message(queue_name, wait_time=10):
    """ Returns 0 or 1 message from the queue"""
    try:
        queue = get_sqs_queue_resource(queue_name=queue_name)
        sqs_message = queue.receive_messages(
            WaitTimeSeconds=wait_time,
            MessageAttributeNames=["All"],
            VisibilityTimeout=DEFAULT_VISIBILITY_TIMEOUT,
            MaxNumberOfMessages=1,
        )
    except botocore.exceptions.ClientError:
        logger.exception("SQS connection issue. Investigate settings")
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
