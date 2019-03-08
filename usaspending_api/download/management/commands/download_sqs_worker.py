import botocore
import os
import signal
import time

from multiprocessing import Process

from django.conf import settings
from django.core.management.base import BaseCommand

from usaspending_api.common.sqs_helpers import get_sqs_queue_resource
from usaspending_api.download.download_exceptions import FatalError
from usaspending_api.download.filestreaming import csv_generation
from usaspending_api.download.helpers import write_to_download_log as write_to_log
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.models import DownloadJob

CURRENT_MESSAGE = None
DEFAULT_VISIBILITY_TIMEOUT = 60
LONG_POLL_SECONDS = 10
MONITOR_SLEEP_TIME = 5  # must be significantly less than DEFAULT_VISIBILITY_TIMEOUT


class Command(BaseCommand):
    def handle(self, *args, **options):
        # route signal handling to custom function
        for sig in [signal.SIGINT, signal.SIGQUIT, signal.SIGTERM]:
            signal.signal(sig, signal_handler)

        download_service_manager()


def signal_handler(signum, frame):
    write_to_log({"message": "Received signal ({}). Releasing job to Queue".format(signum)})
    release_sqs_message(CURRENT_MESSAGE)
    raise SystemExit


def download_service_manager():
    global CURRENT_MESSAGE

    while True:
        CURRENT_MESSAGE = poll_queue(long_poll_time=LONG_POLL_SECONDS)

        if not CURRENT_MESSAGE:
            continue

        download_job_id = int(CURRENT_MESSAGE.body)

        # If job, pass ID to new process
        write_to_log(message="Message Received: {}".format(CURRENT_MESSAGE))
        download_app = create_and_start_new_process(download_job_id)

        monitor_process = True
        while monitor_process:
            monitor_process = False
            if download_app.exitcode == 0:  # If Process exits with 0, success. remove from queue
                if remove_message_from_sqs(CURRENT_MESSAGE):
                    CURRENT_MESSAGE = None
            elif download_app.exitcode not in (None, 0):   # If process exits with error, don't retry
                update_download_record(download_job_id, "failed")
            elif not download_app.is_alive():  # Process dies
                update_download_record(download_job_id, "ready")
                release_sqs_message(CURRENT_MESSAGE)
                time.sleep(MONITOR_SLEEP_TIME)  # allow the system to breathe before starting another job
            else:
                # Monitor process. Send heartbeats to SQS
                set_sqs_message_visibility(CURRENT_MESSAGE, DEFAULT_VISIBILITY_TIMEOUT)
                time.sleep(MONITOR_SLEEP_TIME)
                monitor_process = True


def poll_queue(long_poll_time):
    # Future TODO: allow different types of queues (redis, etc)
    return poll_sqs_for_message(settings.BULK_DOWNLOAD_SQS_QUEUE_NAME, long_poll_time)


def poll_sqs_for_message(queue_name, wait_time):
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
        write_to_log(message="SQS connection issue. Investigate settings", is_error=True)
        raise SystemExit(1)

    return sqs_message[0] if sqs_message else None


def create_and_start_new_process(download_job_id):
    download_app = Process(
        name="Download Service Worker Proccess", target=download_service_app, args=(download_job_id,)
    )
    download_app.start()

    return download_app


def download_service_app(download_job_id):
    download_job = retrive_download_job_from_db(download_job_id)
    write_to_log(message="Starting new Download Service App with pid {}".format(os.getpid()), download_job=download_job)

    # Retrieve the data and write to the CSV(s)
    csv_generation.generate_csvs(download_job=download_job)

    return True


def remove_message_from_sqs(message):
    if message is None:
        return
    try:
        message.delete()
    except Exception as e:
        write_to_log(message="Unable to delete SQS message. Message might have been previously released or removed")


def set_sqs_message_visibility(message, new_visibility):
    if message is None:
        return
    try:
        message.change_visibility(VisibilityTimeout=new_visibility)
    except botocore.exceptions.ClientError as e:
        write_to_log(
            message="Unable to edit SQS message VisibilityTimeout. Might have already been released or removed"
        )


def release_sqs_message(message):
    set_sqs_message_visibility(message, 0)


def retrive_download_job_from_db(download_job_id):
    download_job = DownloadJob.objects.filter(download_job_id=download_job_id).first()
    if download_job is None:
        raise FatalError("Download Job {} record missing in DB!".format(download_job_id))
    return download_job


def update_download_record(download_job_id, status, message=None):
    download_job = retrive_download_job_from_db(download_job_id)

    download_job.job_status_id = JOB_STATUS_DICT[status]
    if message:
        download_job.error_message = str(message)

    download_job.save()
