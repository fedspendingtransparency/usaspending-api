import logging
import os
import shutil
from pathlib import Path
from typing import Optional, Union

from django.conf import settings

from usaspending_api.download.helpers import write_to_download_log as write_to_log
from usaspending_api.download.models.download_job import DownloadJob

logger = logging.getLogger(__name__)


def cleanup_previous_download_attempt(
        download_job: DownloadJob,
        working_dir_path: Optional[Union[Path, str]] = None,
        use_logger: bool = False
) -> None:
    """
    Clean up any artifacts left behind from a previous failed download attempt.
    This ensures idempotent retry behavior and prevents duplicate data.

    Artifacts cleaned:
    - DownloadJobLookup entries for this download_job_id (prevents duplicate rows in queries)
    - Temporary working directory and files
    - Incomplete zip files

    Args:
        download_job: The DownloadJob being processed
        working_dir_path: Optional custom working directory path. If None, uses default CSV_LOCAL_PATH
        use_logger: If True, use logger.info/warning. If False, use write_to_log (for non-Spark downloads)
    """
    from usaspending_api.download.models.download_job_lookup import DownloadJobLookup

    download_job_id = download_job.download_job_id

    def log_message(message: str, is_error: bool = False):
        """Helper to log with appropriate method"""
        if use_logger:
            if is_error:
                logger.warning(message)
            else:
                logger.info(message)
        else:
            write_to_log(message=message, download_job=download_job, is_error=is_error)

    # Clean up lookup table entries
    deleted_count, _ = DownloadJobLookup.objects.filter(download_job_id=download_job_id).delete()
    if deleted_count > 0:
        log_message(
            f"Cleaned up {deleted_count} orphaned DownloadJobLookup entries from previous failed attempt"
        )

    # Determine working directory path
    if working_dir_path is None:
        working_dir_path = Path(settings.CSV_LOCAL_PATH)
    elif isinstance(working_dir_path, str):
        working_dir_path = Path(working_dir_path)

    # Clean up temporary files if they exist
    zip_file_path = working_dir_path / download_job.file_name
    working_dir = Path(str(zip_file_path).rsplit(".", 1)[0])  # Remove extension

    # Remove incomplete zip file
    if zip_file_path.exists():
        try:
            zip_file_path.unlink()
            log_message(
                f"Removed incomplete zip file from previous attempt: {zip_file_path.name}"
            )
        except OSError as e:
            log_message(
                f"Warning: Failed to remove incomplete zip file: {e}",
                is_error=False  # Non-critical, just log as warning
            )

    # Remove incomplete working directory
    if working_dir.exists():
        try:
            shutil.rmtree(working_dir)
            log_message(
                f"Removed incomplete working directory from previous attempt: {working_dir.name}"
            )
        except OSError as e:
            log_message(
                f"Warning: Failed to remove incomplete working directory: {e}",
                is_error=False  # Non-critical, just log as warning
            )

    log_message(f"Pre-download cleanup complete for download_job_id {download_job_id}")


def cleanup_download_files(
        file_paths: list[Union[Path, str]],
        use_logger: bool = False,
        download_job: Optional[DownloadJob] = None
) -> None:
    """
    Clean up files and directories created during download processing.

    Args:
        file_paths: List of file or directory paths to remove
        use_logger: If True, use logger.info. If False, use write_to_log
        download_job: Optional DownloadJob for logging context
    """

    def log_message(message: str):
        """Helper to log with appropriate method"""
        if use_logger:
            logger.info(message)
        else:
            write_to_log(message=message, download_job=download_job)

    for path in file_paths:
        if isinstance(path, str):
            path = Path(path)

        if not path.exists():
            continue

        try:
            if path.is_file():
                log_message(f"Removing file: {path}")
                path.unlink()
            elif path.is_dir():
                log_message(f"Removing directory: {path}")
                shutil.rmtree(path)
        except OSError as e:
            log_message(f"Warning: Failed to remove {path}: {e}")
