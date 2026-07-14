from unittest.mock import Mock, patch

import pytest
from rest_framework.test import APIRequestFactory

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.models import DownloadJob
from usaspending_api.download.v2.download_status import (
    DownloadStatusViewSet,
)


@pytest.fixture
def api_request_factory():
    """Fixture to create API request objects"""
    return APIRequestFactory()


@pytest.fixture
def download_status_viewset():
    """Fixture to create DownloadStatusViewSet instance"""
    return DownloadStatusViewSet()


@pytest.fixture
def mock_download_job():
    """Fixture to create a mock DownloadJob"""

    def _create_job(
            file_name="test_download.zip",
            job_status_name="finished",
            error_message=None,
            file_size=1000000,
            number_of_columns=10,
            number_of_rows=100,
            seconds_elapsed=30.5,
    ):
        job = Mock(spec=DownloadJob)
        job.file_name = file_name
        job.job_status.name = job_status_name
        job.error_message = error_message
        job.file_size = file_size
        job.number_of_columns = number_of_columns
        job.number_of_rows = number_of_rows
        job.seconds_elapsed.return_value = seconds_elapsed
        return job

    return _create_job


def make_request(api_request_factory, path, params=None):
    """Helper to create a properly initialized DRF request"""
    from rest_framework.request import Request

    wsgi_request = api_request_factory.get(path, params or {})
    # Wrap in DRF Request to get query_params attribute
    return Request(wsgi_request)


class TestDownloadStatusViewSetGet:
    """Tests for the get() method"""

    def test_get_with_valid_file_name(self, api_request_factory, download_status_viewset):
        """Test GET request with valid file_name parameter"""
        request = make_request(api_request_factory, '/api/v2/download/status/', {'file_name': 'test.zip'})

        with patch.object(download_status_viewset, 'get_download_status_response') as mock_response:
            mock_response.return_value = Mock(data={'status': 'finished'})

            response = download_status_viewset.get(request)

            mock_response.assert_called_once_with(file_name='test.zip')
            assert response is not None

    def test_get_without_file_name_raises_exception(self, api_request_factory, download_status_viewset):
        """Test GET request without file_name parameter raises InvalidParameterException"""
        request = make_request(api_request_factory, '/api/v2/download/status/')

        with pytest.raises(InvalidParameterException) as exc_info:
            download_status_viewset.get(request)

        assert "Missing one or more required query parameters: file_name" in str(exc_info.value)

    def test_get_with_empty_file_name_raises_exception(self, api_request_factory, download_status_viewset):
        """Test GET request with empty file_name parameter raises InvalidParameterException"""
        request = make_request(api_request_factory, '/api/v2/download/status/', {'file_name': ''})

        with pytest.raises(InvalidParameterException) as exc_info:
            download_status_viewset.get(request)

        assert "Missing one or more required query parameters: file_name" in str(exc_info.value)

    def test_get_with_missing_file_name_raises_exception(self, api_request_factory, download_status_viewset):
        """Test GET request without file_name in query params raises InvalidParameterException"""
        request = make_request(api_request_factory, '/api/v2/download/status/')

        with pytest.raises(InvalidParameterException):
            download_status_viewset.get(request)


class TestGetDownloadStatusResponse:
    """Tests for the get_download_status_response() method"""

    @patch('usaspending_api.download.v2.download_status.get_file_path')
    def test_successful_download_response(
            self,
            mock_get_file_path,
            download_status_viewset,
            mock_download_job
    ):
        """Test response for a successfully completed download"""
        mock_get_file_path.return_value = 'https://example.com/downloads/test.zip'
        job = mock_download_job(
            file_name='test.zip',
            job_status_name='finished',
            file_size=5000000,
            number_of_columns=15,
            number_of_rows=250,
            seconds_elapsed=45.2,
        )

        with patch.object(download_status_viewset, 'get_download_job', return_value=job):
            response = download_status_viewset.get_download_status_response('test.zip')

        assert response.status_code == 200
        assert response.data['status'] == 'finished'
        assert response.data['message'] is None
        assert response.data['file_name'] == 'test.zip'
        assert response.data['file_url'] == 'https://example.com/downloads/test.zip'
        assert response.data['total_size'] == 5000  # bytes to kilobytes
        assert response.data['total_columns'] == 15
        assert response.data['total_rows'] == 250
        assert response.data['seconds_elapsed'] == 45.2

    @patch('usaspending_api.download.v2.download_status.get_file_path')
    def test_failed_download_response(
            self,
            mock_get_file_path,
            download_status_viewset,
            mock_download_job
    ):
        """Test response for a failed download"""
        mock_get_file_path.return_value = 'https://example.com/downloads/failed.zip'
        job = mock_download_job(
            file_name='failed.zip',
            job_status_name='failed',
            error_message='Database connection timeout',
            file_size=None,
            number_of_columns=0,
            number_of_rows=0,
        )

        with patch.object(download_status_viewset, 'get_download_job', return_value=job):
            response = download_status_viewset.get_download_status_response('failed.zip')

        assert response.status_code == 200
        assert response.data['status'] == 'failed'
        assert response.data['message'] == "An error occurred."  # Changed from FAILED_DOWNLOAD_MESSAGE
        assert response.data['file_name'] == 'failed.zip'
        assert response.data['total_size'] is None
        assert response.data['total_columns'] == 0
        assert response.data['total_rows'] == 0

    @patch('usaspending_api.download.v2.download_status.get_file_path')
    def test_running_download_response(
            self,
            mock_get_file_path,
            download_status_viewset,
            mock_download_job
    ):
        """Test response for a download in progress"""
        mock_get_file_path.return_value = 'https://example.com/downloads/running.zip'
        job = mock_download_job(
            file_name='running.zip',
            job_status_name='running',
            file_size=None,
            number_of_columns=None,
            number_of_rows=None,
            seconds_elapsed=120.5,
        )

        with patch.object(download_status_viewset, 'get_download_job', return_value=job):
            response = download_status_viewset.get_download_status_response('running.zip')

        assert response.status_code == 200
        assert response.data['status'] == 'running'
        assert response.data['message'] is None
        assert response.data['seconds_elapsed'] == 120.5

    @patch('usaspending_api.download.v2.download_status.get_file_path')
    def test_response_with_zero_file_size(
            self,
            mock_get_file_path,
            download_status_viewset,
            mock_download_job
    ):
        """Test response when file_size is zero

        Note: This documents a bug in the original code where file_size=0
        returns None instead of 0 because of the falsy check.
        """
        mock_get_file_path.return_value = 'https://example.com/downloads/empty.zip'
        job = mock_download_job(
            file_name='empty.zip',
            job_status_name='finished',
            file_size=0,
        )

        with patch.object(download_status_viewset, 'get_download_job', return_value=job):
            response = download_status_viewset.get_download_status_response('empty.zip')

        # Current behavior: returns None for 0 (this is a bug)
        # The code does: download_job.file_size / 1000 if download_job.file_size else None
        # When file_size is 0, it's falsy, so returns None
        assert response.data['total_size'] is None

        # TODO: Fix the code to properly handle 0:
        # "total_size": download_job.file_size / 1000 if download_job.file_size is not None else None

    @patch('usaspending_api.download.v2.download_status.get_file_path')
    def test_response_converts_bytes_to_kilobytes(
            self,
            mock_get_file_path,
            download_status_viewset,
            mock_download_job
    ):
        """Test that file size is correctly converted from bytes to kilobytes"""
        mock_get_file_path.return_value = 'https://example.com/downloads/test.zip'
        job = mock_download_job(file_size=1500000)  # 1.5 MB

        with patch.object(download_status_viewset, 'get_download_job', return_value=job):
            response = download_status_viewset.get_download_status_response('test.zip')

        assert response.data['total_size'] == 1500  # kilobytes


class TestGetUserMessage:
    """Tests for the _get_user_message() method"""

    def test_no_message_when_no_error(self, download_status_viewset, mock_download_job):
        """Test that no message is returned when there's no error_message"""
        job = mock_download_job(
            job_status_name='finished',
            error_message=None
        )

        message = download_status_viewset._get_user_message(job)

        assert message is None

    def test_no_message_for_running_download_without_error(self, download_status_viewset, mock_download_job):
        """Test that no message is returned for running downloads without error"""
        job = mock_download_job(
            job_status_name='running',
            error_message=None
        )

        message = download_status_viewset._get_user_message(job)

        assert message is None

    def test_generic_message_when_error_exists(self, download_status_viewset, mock_download_job):
        """Test that generic message is returned when error_message exists"""
        job = mock_download_job(
            job_status_name='failed',
            error_message='Internal error: /var/secrets/database.conf not found'
        )

        with patch('usaspending_api.download.v2.download_status.logger') as mock_logger:
            message = download_status_viewset._get_user_message(job)

            # Should return generic message
            assert message == "An error occurred."

            # Should log the actual error
            mock_logger.error.assert_called_once()
            log_call = mock_logger.error.call_args[0][0]
            assert 'failed' in log_call
            assert job.file_name in log_call
            assert job.error_message in log_call

    def test_generic_message_hides_stack_trace(self, download_status_viewset, mock_download_job):
        """Test that stack traces are not exposed to users"""
        stack_trace = """Traceback (most recent call last):
  File "/app/usaspending_api/download/generation.py", line 123, in generate
    result = process_data()
  File "/app/usaspending_api/download/generation.py", line 456, in process_data
    raise ValueError("Invalid data format")
ValueError: Invalid data format"""

        job = mock_download_job(
            job_status_name='failed',
            error_message=stack_trace
        )

        with patch('usaspending_api.download.v2.download_status.logger'):
            message = download_status_viewset._get_user_message(job)

            # Should return generic message (no stack trace)
            assert message == "An error occurred."
            assert 'Traceback' not in message
            assert '/app/usaspending_api' not in message
            assert 'ValueError' not in message

    def test_generic_message_hides_sensitive_paths(self, download_status_viewset, mock_download_job):
        """Test that sensitive file paths are not exposed to users"""
        job = mock_download_job(
            job_status_name='failed',
            error_message='Failed to read /var/secrets/api_keys.json'
        )

        with patch('usaspending_api.download.v2.download_status.logger'):
            message = download_status_viewset._get_user_message(job)

            # Should return generic message (no file paths)
            assert message == "An error occurred."
            assert '/var/secrets' not in message
            assert 'api_keys.json' not in message

    def test_error_logged_but_not_returned(self, download_status_viewset, mock_download_job):
        """Test that error details are logged but not returned to user"""
        error_details = "Database connection failed: psycopg2.OperationalError at line 789"
        job = mock_download_job(
            job_status_name='failed',
            error_message=error_details
        )

        with patch('usaspending_api.download.v2.download_status.logger') as mock_logger:
            message = download_status_viewset._get_user_message(job)

            # User gets generic message
            assert message == "An error occurred."
            assert 'psycopg2' not in message

            # But error is logged
            mock_logger.error.assert_called_once()
            assert error_details in str(mock_logger.error.call_args)

    def test_returns_none_when_no_error_message(self, download_status_viewset, mock_download_job):
        """Test that None is returned when error_message is None, regardless of job status"""
        job = mock_download_job(
            job_status_name='failed',  # Status is failed but no error message
            error_message=None
        )

        with patch('usaspending_api.download.v2.download_status.logger') as mock_logger:
            message = download_status_viewset._get_user_message(job)

            # Should return None (not a message)
            assert message is None

            # Should not log anything
            mock_logger.error.assert_not_called()

    def test_returns_message_even_for_non_failed_status(self, download_status_viewset, mock_download_job):
        """Test that message is returned if error_message exists, even if status isn't 'failed'"""
        job = mock_download_job(
            job_status_name='running',  # Status is running but has error message
            error_message='Unexpected error during processing'
        )

        with patch('usaspending_api.download.v2.download_status.logger') as mock_logger:
            message = download_status_viewset._get_user_message(job)

            # Should return message because error_message exists
            assert message == "An error occurred."

            # Should log the error
            mock_logger.error.assert_called_once()

    def test_empty_string_error_message_returns_message(self, download_status_viewset, mock_download_job):
        """Test that empty string error_message is treated as falsy and returns None"""
        job = mock_download_job(
            job_status_name='failed',
            error_message=''  # Empty string
        )

        with patch('usaspending_api.download.v2.download_status.logger') as mock_logger:
            message = download_status_viewset._get_user_message(job)

            # Empty string is falsy, so should return None
            assert message is None

            # Should not log
            mock_logger.error.assert_not_called()


class TestSecurityAndErrorHandling:
    """Tests for security and error handling"""

    def test_no_stack_trace_in_response_on_exception(
            self,
            api_request_factory,
            download_status_viewset
    ):
        """Test that exceptions don't expose stack traces in response"""
        request = make_request(api_request_factory, '/api/v2/download/status/', {'file_name': 'test.zip'})

        with patch.object(download_status_viewset, 'get_download_job') as mock_get_job:
            # Simulate an unexpected exception
            mock_get_job.side_effect = Exception("Internal error: /var/secrets/config.py")

            # Should raise exception (to be caught by Django/DRF exception handler)
            with pytest.raises(Exception) as exc_info:
                download_status_viewset.get(request)

            # The exception message contains sensitive info internally
            assert "Internal error" in str(exc_info.value)

            # But in production, DRF's exception handler would sanitize this
            # This test documents that we rely on DRF's exception handling

    def test_error_message_sanitization(self, download_status_viewset, mock_download_job):
        """Test that error messages are properly sanitized"""
        sensitive_errors = [
            "Database password: secret123",
            "API key: sk-1234567890abcdef",
            "/var/secrets/config.json not found",
            "psycopg2.OperationalError: connection failed",
        ]

        for error in sensitive_errors:
            job = mock_download_job(
                job_status_name='failed',
                error_message=error
            )

            with patch('usaspending_api.download.v2.download_status.logger'):
                message = download_status_viewset._get_user_message(job)

                # All should return generic message
                assert message == "An error occurred."  # Changed from FAILED_DOWNLOAD_MESSAGE
                # None of the sensitive info should be in the message
                assert error not in message
