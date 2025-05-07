# Standard library imports
import logging
import time
import traceback

# Third-party library imports
from opentelemetry.trace import SpanKind, Status, StatusCode

# Django imports
from django.core.management.base import BaseCommand

# Application imports
from usaspending_api.common.logging import configure_logging
from usaspending_api.common.sqs.sqs_handler import get_sqs_queue
from usaspending_api.common.sqs.sqs_job_logging import log_job_message
from usaspending_api.common.sqs.sqs_work_dispatcher import (
    QueueWorkDispatcherError,
    QueueWorkerProcessError,
    SQSWorkDispatcher,
)
from usaspending_api.common.tracing import SubprocessTrace
from usaspending_api.download.filestreaming.download_generation import generate_download
from usaspending_api.download.helpers.monthly_helpers import download_job_to_log_dict
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.models.download_job import DownloadJob
from usaspending_api.settings import TRACE_ENV


logger = logging.getLogger(__name__)
JOB_TYPE = "USAspendingDownloader"


class Command(BaseCommand):
    def handle(self, *args, **options):
        configure_logging(service_name="usaspending-downloader-" + TRACE_ENV)
        # Start a main trace for the SQS worker session
        with SubprocessTrace(
            name=f"job.{JOB_TYPE}.download_sqs_worker",
            kind=SpanKind.INTERNAL,
            service="bulk-download",
        ) as parent:
            parent.set_attributes(
                {
                    "service": "bulk-download",
                    "job_type": str(JOB_TYPE),
                    "message": "Starting SQS worker session",
                }
            )

            # Creates a Context object with parent set as current span
            # any Child span using .start_span() will automatically be the child of the parent
            # Refer to this link: https://github.com/open-telemetry/opentelemetry-python/issues/2787

            queue = get_sqs_queue()
            log_job_message(logger=logger, message="Starting SQS polling", job_type=JOB_TYPE)

            message_found = None
            keep_polling = True

            while keep_polling:
                # Setup dispatcher that coordinates job activity on SQS
                dispatcher = SQSWorkDispatcher(
                    queue,
                    worker_process_name=JOB_TYPE,
                    worker_can_start_child_processes=True,
                )

                # Mark the job as failed if: there was an error processing the download; retries after interrupt
                # are not allowed; or all retries have been exhausted
                # If the job is interrupted by an OS signal, the dispatcher's signal handling logic will log and
                # handle this case
                # Retries are allowed or denied by the SQS queue's RedrivePolicy config
                # That is, if maxReceiveCount > 1 in the policy, then retries are allowed
                # - if queue retries are allowed, the queue message will retry to the max allowed by the queue
                # - As coded, no cleanup should be needed to retry a download
                #   - the psql -o will overwrite the output file
                #   - the zip will use 'w' write mode to create from scratch each time
                # The worker function controls the maximum allowed runtime of the job

                try:
                    # Check the queue for work and hand it to the given processing function
                    message_found = dispatcher.dispatch(download_service_app)

                    # Start a new span only if a message is found
                    if message_found:
                        with SubprocessTrace(
                            name=f"job.{JOB_TYPE}.message_processing",
                            kind=SpanKind.INTERNAL,
                            service="bulk-download",
                        ) as poll_span:
                            poll_span.set_attributes(
                                {
                                    "service": "bulk-download",
                                    "job_type": str(JOB_TYPE),
                                    "resource": str(queue.url),
                                    "span_type": "Internal",
                                    "message": "Processing download job message",
                                }
                            )

                except (QueueWorkerProcessError, QueueWorkDispatcherError) as exc:
                    _handle_queue_error(exc)

                    # Start a span only for the error, capturing details
                    with SubprocessTrace(
                        name=f"job.{JOB_TYPE}.error_handling",
                        kind=SpanKind.INTERNAL,
                        service="bulk-download",
                    ) as error_span:
                        error_span.set_attributes(
                            {
                                "service": "bulk-download",
                                "span_type": "Internal",
                                "message": "Error processing message",
                                "error": str(exc),
                            }
                        )
                        error_span.set_status(Status(StatusCode.ERROR, str(exc)))

                if not message_found:
                    # Not using the EagerlyDropTraceFilter since we are no longer sending traces every iteration. Only when actionabe events happen
                    time.sleep(1)

                # If this process is exiting, don't poll for more work
                keep_polling = not dispatcher.is_exiting


def download_service_app(download_job_id):

    download_job = _retrieve_download_job_from_db(download_job_id)

    _log_and_trace_download_job("Starting processing of download request", download_job)

    generate_download(download_job=download_job)


def _retrieve_download_job_from_db(download_job_id):

    download_job = DownloadJob.objects.filter(download_job_id=download_job_id).first()
    if download_job is None:
        raise DownloadJobNoneError(download_job_id)

    _log_and_trace_download_job("Retreiving Download Job From DB", download_job)
    return download_job


def _update_download_job_status(download_job_id, status, error_message=None, overwrite_error_message=False):
    """Handles status updates on the DownloadJob records

    Args:
        download_job_id (int): id of the DownloadJob record being processed
        status (str): a status key to JOB_STATUS_DICT dictionary
        error_message (str): error message to potentially apply to the DownloadJob record if
            ``overwrite_error_message`` is True or if there is no prior error message on the record
        overwrite_error_message (bool): if explicitly set to True, and error_message is not None, the provided error
            message will overwrite an existing error message on the DownloadJob object. By default it will not.
    """

    download_job = _retrieve_download_job_from_db(download_job_id)

    _log_and_trace_download_job("Updating Download Job Status", download_job)

    download_job.job_status_id = JOB_STATUS_DICT[status]
    if overwrite_error_message or (error_message and not download_job.error_message):
        download_job.error_message = str(error_message) if error_message else None

    download_job.save()


def _handle_queue_error(exc):
    """Handles exceptions raised when processing a message from the queue.

    It handles two types:
        QueueWorkerProcessError: an error in the job-processing code
        QueueWorkDispatcherError: exception raised in the job-processing code

    Try to mark the job as failed, but continue raising the original Exception if not possible
    This has likely already been set on the job from within error-handling code of the target of the
    worker process, but this is here to for redundancy
    """
    try:
        download_job_id = None
        if exc.queue_message and exc.queue_message.body:
            download_job_id = int(exc.queue_message.body)
        log_job_message(
            logger=logger,
            message=f"{type(exc).__name__} caught. Attempting to fail the DownloadJob with id = {download_job_id}",
            job_type=JOB_TYPE,
            job_id=download_job_id,
            is_exception=True,
        )
        if download_job_id:
            stack_trace = "".join(traceback.format_exception(type(exc), value=exc, tb=exc.__traceback__))
            status = "failed"
            try:
                _update_download_job_status(download_job_id, status, stack_trace)
                log_job_message(
                    logger=logger,
                    message=f"Marked DownloadJob with id = {download_job_id} as {status}",
                    job_type=JOB_TYPE,
                    job_id=download_job_id,
                    is_exception=True,
                )
            except Exception as e:
                log_job_message(
                    logger=logger,
                    message=f"Unable to mark DownloadJob with id = {download_job_id} as {status}",
                    job_type=JOB_TYPE,
                    job_id=download_job_id,
                    is_exception=True,
                )
                raise e

    except:  # noqa
        pass
    raise exc


def _log_and_trace_download_job(message: str, download_job: DownloadJob):
    """
    Logs information about a download_job and pulls information of out it to send
    as a trace.
    """

    download_job_details = download_job_to_log_dict(download_job)

    log_job_message(
        logger=logger,
        message=message,
        job_type=JOB_TYPE,
        job_id=download_job.download_job_id,
        other_params=download_job_details,  # download job details
    )

    with SubprocessTrace(
        name=f"job.{JOB_TYPE}.download.update",
        kind=SpanKind.INTERNAL,
        service="bulk-download",
    ) as span:
        span.set_attributes(
            {
                "message": message,
                "service": "bulk-download",
                "span_type": str(SpanKind.INTERNAL),
                "job_type": str(JOB_TYPE),
                # download job details
                "download_job_id": str(download_job.download_job_id),
                "download_job_status": str(download_job.job_status.name),  # Convert to relevant field as str
                "download_file_name": str(download_job.file_name),  # Extract specific field as str
                "download_file_size": (
                    download_job.file_size if download_job.file_size is not None else 0
                ),  # int or fallback to 0
                "number_of_rows": download_job.number_of_rows if download_job.number_of_rows is not None else 0,
                "number_of_columns": (
                    download_job.number_of_columns if download_job.number_of_columns is not None else 0
                ),
                "error_message": download_job.error_message if download_job.error_message else "",
                "monthly_download": str(download_job.monthly_download),  # Convert boolean to str
                "json_request": str(download_job.json_request) if download_job.json_request else "",
            }
        )


class DownloadJobNoneError(ValueError):
    """Custom fatal exception representing the scenario where the DownloadJob object to be processed does not
    exist in the database with the given ID.
    """

    def __init__(self, download_job_id, *args, **kwargs):
        default_message = f"DownloadJob with id = {download_job_id} was not found in the database."

        if args or kwargs:
            super().__init__(*args, **kwargs)
        else:
            super().__init__(default_message)
        self.download_job_id = download_job_id
