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


class TestHandleQueueError:

    def test_handle_queue_error_with_valid_message(self, mock_download_job):
        """Test error handler correctly parses valid JSON message body."""
        # Create a mock exception with a valid message body
        exc = QueueWorkerProcessError("Test error")
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

            with pytest.raises(QueueWorkerProcessError):
                _handle_queue_error(exc)

            # Verify the job was marked as failed with the correct ID
            mock_update.assert_called_once()
            assert mock_update.call_args[0][0] == 123  # download_job_id
            assert mock_update.call_args[0][1] == "failed"  # status

    def test_handle_queue_error_with_spark_message(self):
        """Test error handler works with Spark messages containing job_name."""
        exc = QueueWorkerProcessError("Spark job failed")
        exc.queue_message = Mock()
        exc.queue_message.body = json.dumps(
            {
                "download_job_id": 456,
                "download_logic": "spark",
                "job_name": "test-spark-job",
            }
        )

        with (
            patch(
                "usaspending_api.download.management.commands.download_sqs_worker._update_download_job_status"
            ) as mock_update,
            patch("usaspending_api.download.management.commands.download_sqs_worker.log_job_message"),
        ):

            with pytest.raises(QueueWorkerProcessError):
                _handle_queue_error(exc)

            mock_update.assert_called_once()
            assert mock_update.call_args[0][0] == 456

    def test_handle_queue_error_with_invalid_json(self):
        """Test error handler gracefully handles malformed JSON (the original bug)."""
        exc = QueueWorkerProcessError("Test error")
        exc.queue_message = Mock()
        exc.queue_message.body = "123"  # Plain integer, not JSON

        with (
            patch(
                "usaspending_api.download.management.commands.download_sqs_worker._update_download_job_status"
            ) as mock_update,
            patch("usaspending_api.download.management.commands.download_sqs_worker.log_job_message"),
        ):

            with pytest.raises(QueueWorkerProcessError):
                _handle_queue_error(exc)

            # Should not attempt to update status since parsing failed
            mock_update.assert_not_called()

    def test_handle_queue_error_with_missing_download_job_id(self):
        """Test error handler when message is missing download_job_id."""
        exc = QueueWorkerProcessError("Test error")
        exc.queue_message = Mock()
        exc.queue_message.body = json.dumps(
            {
                "download_logic": "postgres",
            }
        )

        with (
            patch(
                "usaspending_api.download.management.commands.download_sqs_worker._update_download_job_status"
            ) as mock_update,
            patch("usaspending_api.download.management.commands.download_sqs_worker.log_job_message"),
        ):

            with pytest.raises(QueueWorkerProcessError):
                _handle_queue_error(exc)

            mock_update.assert_not_called()

    def test_handle_queue_error_no_message_body(self):
        """Test error handler when queue_message has no body."""
        exc = QueueWorkerProcessError("Test error")
        exc.queue_message = Mock()
        exc.queue_message.body = None

        with (
            patch(
                "usaspending_api.download.management.commands.download_sqs_worker._update_download_job_status"
            ) as mock_update,
            patch("usaspending_api.download.management.commands.download_sqs_worker.log_job_message"),
        ):

            with pytest.raises(QueueWorkerProcessError):
                _handle_queue_error(exc)

            mock_update.assert_not_called()

    def test_handle_queue_error_no_queue_message(self):
        """Test error handler when exception has no queue_message."""
        exc = QueueWorkerProcessError("Test error")
        exc.queue_message = None

        with (
            patch(
                "usaspending_api.download.management.commands.download_sqs_worker._update_download_job_status"
            ) as mock_update,
            patch("usaspending_api.download.management.commands.download_sqs_worker.log_job_message"),
        ):

            with pytest.raises(QueueWorkerProcessError):
                _handle_queue_error(exc)

            mock_update.assert_not_called()

    def test_handle_queue_error_update_fails(self):
        """Test error handler when _update_download_job_status raises an exception."""
        exc = QueueWorkerProcessError("Test error")
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

            mock_update.side_effect = Exception("Database error")

            # Should still raise the original exception, not the update error
            with pytest.raises(QueueWorkerProcessError) as exc_info:
                _handle_queue_error(exc)

            assert str(exc_info.value) == "Test error"

    def test_handle_queue_error_with_dispatcher_error(self):
        """Test error handler works with QueueWorkDispatcherError."""
        exc = QueueWorkDispatcherError("Dispatcher error")
        exc.queue_message = Mock()
        exc.queue_message.body = json.dumps(
            {
                "download_job_id": 789,
                "download_logic": "postgres",
            }
        )

        with (
            patch(
                "usaspending_api.download.management.commands.download_sqs_worker._update_download_job_status"
            ) as mock_update,
            patch("usaspending_api.download.management.commands.download_sqs_worker.log_job_message"),
        ):

            with pytest.raises(QueueWorkDispatcherError):
                _handle_queue_error(exc)

            mock_update.assert_called_once()
            assert mock_update.call_args[0][0] == 789
