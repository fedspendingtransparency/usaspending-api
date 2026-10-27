import json
from unittest.mock import Mock, patch

import pytest

from usaspending_api.common.sqs.sqs_work_dispatcher import (
    QueueWorkDispatcherError,
    QueueWorkerProcessError,
)
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.management.commands.download_sqs_worker import _handle_queue_error
from usaspending_api.download.models.download_job import DownloadJob


@pytest.fixture
def mock_download_job():
    """Create a mock DownloadJob for testing."""
    job = Mock(spec=DownloadJob)
    job.download_job_id = 123
    job.job_status_id = JOB_STATUS_DICT["ready"]
    job.error_message = None
    job.file_name = "test_download.zip"
    job.file_size = 1024
    job.number_of_rows = 100
    job.number_of_columns = 10
    job.monthly_download = False
    job.json_request = '{"test": "data"}'
    job.job_status = Mock(name="ready")
    return job


class TestHandleErrorMessage:

    
    def test_handle_queue_error_update_fails(self):
        """Test error message during download does not contain connection string."""
        exc = QueueWorkerProcessError("Database error postgresql://*")
        exc.queue_message = Mock()
        exc.queue_message.body = json.dumps(
            {
                "download_job_id": 123,
                "download_logic": "postgres",
            }
        )

        with (
            patch(
                "usaspending_api.download.management.commands.download_sqs_worker._update_download_job_status"
            ) as mock_update,
            patch("usaspending_api.download.management.commands.download_sqs_worker.log_job_message"),
        ):

            mock_update.side_effect = Exception("Database error postgresql://")

            with pytest.raises(QueueWorkerProcessError) as exc_info:
                _handle_queue_error(exc)

            assert "*" in str(exc_info.value) 
