import logging

from usaspending_api.references.models import ToptierAgency

logger = logging.getLogger(__name__)


def write_to_download_log(
    message, job_type="USAspendingDownloader", download_job=None, is_debug=False, is_error=False, other_params=None
):
    """Handles logging for the downloader instance"""
    if not other_params:
        other_params = {}

    log_dict = {"message": message}

    download_job_to_log_dict(download_job, log_dict)

    for param in other_params:
        if param not in log_dict:
            log_dict[param] = other_params[param]

    if is_error:
        log_dict["message_type"] = f"{job_type}Error"
        logger.exception(log_dict)
    elif is_debug:
        log_dict["message_type"] = f"{job_type}Debug"
        logger.debug(log_dict)
    else:
        log_dict["message_type"] = f"{job_type}Info"
        logger.info(log_dict)


def download_job_to_log_dict(download_job, log_dict=None):
    if not log_dict:
        log_dict = {}
    if download_job:
        log_dict["download_job_id"] = download_job.download_job_id
        log_dict["job_id"] = download_job.download_job_id  # to match log structure of generic queued jobs
        log_dict["file_name"] = download_job.file_name
        log_dict["json_request"] = download_job.json_request
        if download_job.error_message:
            log_dict["error_message"] = download_job.error_message
    return log_dict


def pull_modified_agencies_cgacs():
    return ToptierAgency.objects.filter(agency__user_selectable=True).values_list("toptier_code", flat=True)
