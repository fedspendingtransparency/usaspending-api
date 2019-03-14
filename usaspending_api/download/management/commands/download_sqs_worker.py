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

BSD_SIGNALS = {
    1: "SIGHUP [1] (Hangup detected on controlling terminal or death of controlling process)",
    2: "SIGINT [2] (Interrupt from keyboard)",
    3: "SIGQUIT [3] (Quit from keyboard)",
    4: "SIGILL [4] (Illegal Instruction)",
    6: "SIGABRT [6] (Abort signal from abort(3))",
    8: "SIGFPE [8] (Floating point exception)",
    9: "SIGKILL [9] (non-catchable, non-ignorable kill)",
    11: "SIGSEGV [11] (Invalid memory reference)",
    14: "SIGALRM [12] (Timer signal from alarm(2))",
    15: "SIGTERM [15] (software termination signal)",
}
current_message = None
DEFAULT_VISIBILITY_TIMEOUT = 60
LONG_POLL_SECONDS = 10
MONITOR_SLEEP_TIME = 5  # This needs to be less than DEFAULT_VISIBILITY_TIMEOUT


class Command(BaseCommand):
    def handle(self, *args, **options):
        if MONITOR_SLEEP_TIME >= DEFAULT_VISIBILITY_TIMEOUT:
            msg = "MONITOR_SLEEP_TIME must be less than DEFAULT_VISIBILITY_TIMEOUT. Otherwise job duplication can occur"
            raise Exception(msg)
        for sig in [signal.SIGINT, signal.SIGQUIT, signal.SIGTERM]:
            signal.signal(sig, signal_handler)  # route signal handling to custom function
        download_service_manager()


def signal_handler(signum, frame):
    """
        Custom handler code to execute when the process receives a signal.
        Allows the script to update/release jobs and then gracefully exit.
    """
    # If the signal is in BSD_SIGNALS, use the human-readable string, otherwise use the signal value
    signal_or_human = BSD_SIGNALS.get(signum, signum)
    write_to_log({"message": "Received signal ({}). Gracefully stopping Download Job".format(signal_or_human)})
    surrender_sqs_message_to_other_clients(current_message)
    raise SystemExit  # quietly end parent process


def download_service_manager():
    global current_message

    while True:
        current_message = poll_queue(long_poll_time=LONG_POLL_SECONDS)

        if not current_message:
            continue

        download_job_id = int(current_message.body)

        write_to_log(message="Message Received: {}".format(current_message))
        download_app = create_and_start_new_process(download_job_id)

        monitor_process = True
        while monitor_process:
            monitor_process = False
            if download_app.is_alive():
                # Monitor process. Send heartbeats to SQS
                set_sqs_message_visibility(current_message, DEFAULT_VISIBILITY_TIMEOUT)
                time.sleep(MONITOR_SLEEP_TIME)
                monitor_process = True
            elif download_app.exitcode == 0:  # If process exits with 0: success! Remove from queue
                remove_message_from_sqs(current_message)
                current_message = None
            elif download_app.exitcode > 0:  # If process exits with positive code, there was an error. Don't retry
                write_to_log(
                    message="Download Job ({}) Process existed with {}.".format(download_job_id, download_app.exitcode)
                )
                update_download_record(download_job_id, "failed")
            elif download_app.exitcode < 0:
                """ If process exits with a negative code, process was terminated by a signal since
                a Python subprocess returns the negative value of the signal.
                If the signal is in BSD_SIGNALS, use the human-readable string, otherwise use the signal value"""
                signum = download_app.exitcode * -1
                signal_or_human = BSD_SIGNALS.get(signum, signum)
                write_to_log(
                    message="Download Job ({}) Process existed with {}.".format(download_job_id, signal_or_human)
                )
                update_download_record(download_job_id, "ready")
                surrender_sqs_message_to_other_clients(current_message)
                time.sleep(MONITOR_SLEEP_TIME)  # Wait. System might be shutting down.


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
    download_job = retrieve_download_job_from_db(download_job_id)
    write_to_log(message="Starting new Download Service App with pid {}".format(os.getpid()), download_job=download_job)

    # Retrieve the data and write to the CSV(s)
    try:
        csv_generation.generate_csvs(download_job=download_job)
    except Exception as e:
        write_to_log(message="Caught exception", download_job=download_job, is_error=True)
        return 11  # arbitrary positive integer

    return 0


def remove_message_from_sqs(message):
    if message is None:
        return
    try:
        message.delete()
    except Exception as e:
        write_to_log(message="Unable to delete SQS message. Message might have previously been released or removed")


def set_sqs_message_visibility(message, new_visibility):
    if message is None:
        write_to_log(message="No SQS message to modify. Message might have previously been released or removed")
        return
    try:
        message.change_visibility(VisibilityTimeout=new_visibility)
    except botocore.exceptions.ClientError as e:
        write_to_log(message="Unable to set VisibilityTimeout. Message might have previously been released or removed")


def surrender_sqs_message_to_other_clients(message):
    # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
    set_sqs_message_visibility(message, 0)


def retrieve_download_job_from_db(download_job_id):
    download_job = DownloadJob.objects.filter(download_job_id=download_job_id).first()
    if download_job is None:
        raise FatalError("Download Job {} record missing in DB!".format(download_job_id))
    return download_job


def update_download_record(download_job_id, status, error_message=None):
    download_job = retrieve_download_job_from_db(download_job_id)

    download_job.job_status_id = JOB_STATUS_DICT[status]
    if error_message:
        download_job.error_message = str(error_message)

    download_job.save()
