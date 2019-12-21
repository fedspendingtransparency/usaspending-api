import os


def log_job_message(
    logger,
    message,
    job_type,
    job_id=None,
    is_debug=False,
    is_warning=False,
    is_error=False,
    is_exception=False,
    other_params=None,
):
    """Handles logging a message about a Bulk Download job, with additional job metadata"""
    if not other_params:
        other_params = {}

    log_dict = {
        "job_type": job_type,
        "job_id": job_id,
        "proc_id": os.getpid(),
        "parent_proc_id": os.getppid(),
        "message": message,
    }

    for param in other_params:
        if param not in log_dict:
            log_dict[param] = other_params[param]

    if is_exception:  # use this when handling an exception to include exception details in log output
        log_dict["message_type"] = f"{job_type}Error"
        logger.exception(log_dict)
    elif is_error:
        log_dict["message_type"] = f"{job_type}Error"
        logger.error(log_dict)
    elif is_warning:
        log_dict["message_type"] = f"{job_type}Warning"
        logger.warning(log_dict)
    elif is_debug:
        log_dict["message_type"] = f"{job_type}Debug"
        logger.debug(log_dict)
    else:
        log_dict["message_type"] = f"{job_type}Info"
        logger.info(log_dict)
