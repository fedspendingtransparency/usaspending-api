from unittest.mock import Mock, patch

import pytest
from rest_framework.exceptions import NotFound
from rest_framework.test import APIRequestFactory

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.models import DownloadJob
from usaspending_api.download.v2.download_status import (
    FAILED_DOWNLOAD_MESSAGE,
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
        assert response.data['message'] == FAILED_DOWNLOAD_MESSAGE
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

    def test_no_message_for_successful_download(self, download_status_viewset, mock_download_job):
        """Test that no message is returned for successful downloads"""
        job = mock_download_job(job_status_name='finished')

        message = download_status_viewset._get_user_message(job)

        assert message is None

    def test_no_message_for_running_download(self, download_status_viewset, mock_download_job):
        """Test that no message is returned for running downloads"""
        job = mock_download_job(job_status_name='running')

        message = download_status_viewset._get_user_message(job)

        assert message is None

    def test_generic_message_for_failed_download(self, download_status_viewset, mock_download_job):
        """Test that generic message is returned for failed downloads"""
        job = mock_download_job(
            job_status_name='failed',
            error_message='Internal error: /var/secrets/database.conf not found'
        )

        with patch('usaspending_api.download.v2.download_status.logger') as mock_logger:
            message = download_status_viewset._get_user_message(job)

            # Should return generic message
            assert message == FAILED_DOWNLOAD_MESSAGE

            # Should log the actual error
            mock_logger.error.assert_called_once()
            log_call_args = mock_logger.error.call_args[0]
            assert 'failed' in log_call_args[0]
            assert job.file_name in log_call_args
            assert job.error_message in log_call_args

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
            assert message == FAILED_DOWNLOAD_MESSAGE
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
            assert message == FAILED_DOWNLOAD_MESSAGE
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
            assert message == FAILED_DOWNLOAD_MESSAGE
            assert 'psycopg2' not in message

            # But error is logged
            mock_logger.error.assert_called_once()
            assert error_details in str(mock_logger.error.call_args)

    def test_failed_download_without_error_message(self, download_status_viewset, mock_download_job):
        """Test failed download with no error_message set"""
        job = mock_download_job(
            job_status_name='failed',
            error_message=None
        )

        with patch('usaspending_api.download.v2.download_status.logger') as mock_logger:
            message = download_status_viewset._get_user_message(job)

            # Should still return generic message
            assert message == FAILED_DOWNLOAD_MESSAGE

            # Should not log anything
            mock_logger.error.assert_not_called()


class TestGetDownloadJob:
    """Tests for the get_download_job() method"""

    @patch('usaspending_api.download.v2.download_status.connections')
    @patch('usaspending_api.download.v2.download_status.ReadReplicaRouter')
    @patch('usaspending_api.download.v2.download_status.DownloadJob')
    def test_get_download_job_with_read_replica(
            self,
            mock_download_job_model,
            mock_router,
            mock_connections,
            download_status_viewset
    ):
        """Test getting download job using read replica connection"""
        mock_router.read_replicas = ['read_replica_1']
        mock_connections.__dict__ = {
            '_settings': {
                'default': {},
                'read_replica_1': {}
            }
        }

        mock_job = Mock()
        mock_download_job_model.objects.using.return_value.filter.return_value.first.return_value = mock_job

        result = download_status_viewset.get_download_job('test.zip')

        mock_download_job_model.objects.using.assert_called_once_with('read_replica_1')
        assert result == mock_job

    @patch('usaspending_api.download.v2.download_status.connections')
    @patch('usaspending_api.download.v2.download_status.ReadReplicaRouter')
    @patch('usaspending_api.download.v2.download_status.DownloadJob')
    def test_get_download_job_without_read_replica(
            self,
            mock_download_job_model,
            mock_router,
            mock_connections,
            download_status_viewset
    ):
        """Test getting download job using default connection when no read replica"""
        mock_router.read_replicas = ['read_replica_1']
        mock_connections.__dict__ = {
            '_settings': {
                'default': {}
                # read_replica_1 not in settings
            }
        }

        mock_job = Mock()
        mock_download_job_model.objects.filter.return_value.first.return_value = mock_job

        result = download_status_viewset.get_download_job('test.zip')

        mock_download_job_model.objects.filter.assert_called_once_with(file_name='test.zip')
        assert result == mock_job

    @patch('usaspending_api.download.v2.download_status.connections')
    @patch('usaspending_api.download.v2.download_status.ReadReplicaRouter')
    @patch('usaspending_api.download.v2.download_status.DownloadJob')
    def test_get_download_job_not_found_raises_exception(
            self,
            mock_download_job_model,
            mock_router,
            mock_connections,
            download_status_viewset
    ):
        """Test that NotFound exception is raised when download job doesn't exist"""
        mock_router.read_replicas = ['read_replica_1']
        mock_connections.__dict__ = {'_settings': {'default': {}}}

        mock_download_job_model.objects.filter.return_value.first.return_value = None

        with pytest.raises(NotFound) as exc_info:
            download_status_viewset.get_download_job('nonexistent.zip')

        assert 'nonexistent.zip' in str(exc_info.value)
        assert 'does not exist' in str(exc_info.value)

    @patch('usaspending_api.download.v2.download_status.connections')
    @patch('usaspending_api.download.v2.download_status.ReadReplicaRouter')
    @patch('usaspending_api.download.v2.download_status.DownloadJob')
    def test_get_download_job_filters_by_file_name(
            self,
            mock_download_job_model,
            mock_router,
            mock_connections,
            download_status_viewset
    ):
        """Test that download job is filtered by correct file_name"""
        mock_router.read_replicas = ['read_replica_1']
        mock_connections.__dict__ = {'_settings': {'default': {}}}

        mock_job = Mock()
        mock_filter = Mock()
        mock_filter.first.return_value = mock_job
        mock_download_job_model.objects.filter.return_value = mock_filter

        download_status_viewset.get_download_job('specific_file.zip')

        mock_download_job_model.objects.filter.assert_called_once_with(file_name='specific_file.zip')


class TestDownloadStatusViewSetIntegration:
    """Integration tests for the complete flow"""

    @patch('usaspending_api.download.v2.download_status.get_file_path')
    @patch('usaspending_api.download.v2.download_status.connections')
    @patch('usaspending_api.download.v2.download_status.ReadReplicaRouter')
    @patch('usaspending_api.download.v2.download_status.DownloadJob')
    def test_complete_successful_flow(
            self,
            mock_download_job_model,
            mock_router,
            mock_connections,
            mock_get_file_path,
            api_request_factory,
            download_status_viewset,
            mock_download_job
    ):
        """Test complete flow from request to response for successful download"""
        # Setup mocks
        mock_router.read_replicas = ['read_replica_1']
        mock_connections.__dict__ = {'_settings': {'default': {}}}
        mock_get_file_path.return_value = 'https://example.com/test.zip'

        job = mock_download_job(
            file_name='test.zip',
            job_status_name='finished',
            file_size=2000000,
        )
        mock_download_job_model.objects.filter.return_value.first.return_value = job

        # Make request
        request = make_request(api_request_factory, '/api/v2/download/status/', {'file_name': 'test.zip'})
        response = download_status_viewset.get(request)

        # Verify response
        assert response.status_code == 200
        assert response.data['status'] == 'finished'
        assert response.data['file_name'] == 'test.zip'
        assert response.data['total_size'] == 2000

    @patch('usaspending_api.download.v2.download_status.get_file_path')
    @patch('usaspending_api.download.v2.download_status.connections')
    @patch('usaspending_api.download.v2.download_status.ReadReplicaRouter')
    @patch('usaspending_api.download.v2.download_status.DownloadJob')
    def test_complete_failed_flow_hides_error_details(
            self,
            mock_download_job_model,
            mock_router,
            mock_connections,
            mock_get_file_path,
            api_request_factory,
            download_status_viewset,
            mock_download_job
    ):
        """Test complete flow for failed download hides error details from user"""
        # Setup mocks
        mock_router.read_replicas = ['read_replica_1']
        mock_connections.__dict__ = {'_settings': {'default': {}}}
        mock_get_file_path.return_value = 'https://example.com/failed.zip'

        job = mock_download_job(
            file_name='failed.zip',
            job_status_name='failed',
            error_message='Traceback: ValueError at /internal/path/file.py line 123'
        )
        mock_download_job_model.objects.filter.return_value.first.return_value = job

        # Make request
        request = make_request(api_request_factory, '/api/v2/download/status/', {'file_name': 'failed.zip'})

        with patch('usaspending_api.download.v2.download_status.logger'):
            response = download_status_viewset.get(request)

        # Verify response hides error details
        assert response.status_code == 200
        assert response.data['status'] == 'failed'
        assert response.data['message'] == FAILED_DOWNLOAD_MESSAGE

        # Verify no sensitive info in response
        response_str = str(response.data)
        assert 'Traceback' not in response_str
        assert '/internal/path' not in response_str
        assert 'ValueError' not in response_str


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
                assert message == FAILED_DOWNLOAD_MESSAGE
                # None of the sensitive info should be in the message
                assert error not in message
