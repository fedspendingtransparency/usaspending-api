import pytest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from rest_framework.test import APIRequestFactory
from rest_framework.decorators import api_view
from rest_framework.response import Response
from usaspending_api.common.api_request_utils import LLMAPIKeyHandler


@api_view(['GET', 'POST'])
@LLMAPIKeyHandler.require_api_key
def mock_llm_view(request):
    """Mock view for testing the decorator"""
    return Response({"success": True, "message": "LLM endpoint accessed"})


@pytest.fixture
def api_rf():
    """Fixture to provide APIRequestFactory"""
    return APIRequestFactory()


class TestLLMAPIKeyHandler:
    """Test suite for LLMAPIKeyHandler.require_api_key decorator"""

    # ========== Acceptance Criteria Test: Header not included ==========
    def test_missing_header_returns_403_with_message(self, api_rf):
        """
        AC: If header isn't included in the API request, return 403 with appropriate message
        """
        request = api_rf.get('/test/')
        response = mock_llm_view(request)

        assert response.status_code == 403
        assert "detail" in response.data
        assert "X-LLM-API-Key header is required" in response.data['detail']

    # ========== Acceptance Criteria Test: AWS doesn't contain secret ==========
    @patch.dict('os.environ', {'LLM_API_SECRET_NAME': 'test-secret-name'})
    @patch('boto3.session.Session')
    def test_secret_not_found_returns_403_with_message(self, mock_session, api_rf):
        """
        AC: If AWS doesn't contain the requested secret, return 403 with appropriate message
        """
        mock_client = MagicMock()
        mock_client.get_secret_value.side_effect = ClientError(
            {'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Secret not found'}},
            'GetSecretValue'
        )
        mock_session.return_value.client.return_value = mock_client

        request = api_rf.get('/test/', HTTP_X_LLM_API_KEY='test-uuid-123')
        response = mock_llm_view(request)

        assert response.status_code == 403
        assert "detail" in response.data
        assert "not found in AWS Secrets Manager" in response.data['detail']
        assert "test-secret-name" in response.data['detail']

    # ========== Acceptance Criteria Test: UUID doesn't match ==========
    @patch.dict('os.environ', {'LLM_API_SECRET_NAME': 'test-secret-name'})
    @patch('boto3.session.Session')
    def test_invalid_uuid_returns_403_with_message(self, mock_session, api_rf):
        """
        AC: If UUID in the header doesn't match the AWS secret, return 403 with appropriate message
        """
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            'SecretString': 'correct-uuid-123'
        }
        mock_session.return_value.client.return_value = mock_client

        request = api_rf.get('/test/', HTTP_X_LLM_API_KEY='wrong-uuid-456')
        response = mock_llm_view(request)

        assert response.status_code == 403
        assert "detail" in response.data
        assert "Invalid LLM API key" in response.data['detail']

    # ========== Acceptance Criteria Test: Environment variable ==========
    @patch.dict('os.environ', {}, clear=True)
    def test_missing_env_variable_returns_403_with_message(self, api_rf):
        """
        AC: AWS secret name should be acquired via an environment variable
        Tests that missing environment variable returns 403
        """
        request = api_rf.get('/test/', HTTP_X_LLM_API_KEY='test-uuid-123')
        response = mock_llm_view(request)

        assert response.status_code == 403
        assert "detail" in response.data
        assert "secret configuration is not set" in response.data['detail']

    # ========== Success Case: Valid UUID ==========
    @patch.dict('os.environ', {'LLM_API_SECRET_NAME': 'test-secret-name'})
    @patch('boto3.session.Session')
    def test_valid_uuid_plain_string_allows_access(self, mock_session, api_rf):
        """
        Test that valid UUID (plain string format) allows access to the endpoint
        """
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            'SecretString': 'correct-uuid-123'
        }
        mock_session.return_value.client.return_value = mock_client

        request = api_rf.get('/test/', HTTP_X_LLM_API_KEY='correct-uuid-123')
        response = mock_llm_view(request)

        assert response.status_code == 200
        assert response.data['success'] is True
        assert "LLM endpoint accessed" in response.data['message']

    @patch.dict('os.environ', {'LLM_API_SECRET_NAME': 'test-secret-name'})
    @patch('boto3.session.Session')
    def test_valid_uuid_json_format_allows_access(self, mock_session, api_rf):
        """
        Test that valid UUID (JSON format with 'uuid' key) allows access
        """
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            'SecretString': '{"uuid": "correct-uuid-456"}'
        }
        mock_session.return_value.client.return_value = mock_client

        request = api_rf.get('/test/', HTTP_X_LLM_API_KEY='correct-uuid-456')
        response = mock_llm_view(request)

        assert response.status_code == 200
        assert response.data['success'] is True

    @patch.dict('os.environ', {'LLM_API_SECRET_NAME': 'test-secret-name'})
    @patch('boto3.session.Session')
    def test_valid_uuid_json_format_llm_api_key_allows_access(self, mock_session, api_rf):
        """
        Test that valid UUID (JSON format with 'LLM_API_KEY' key) allows access
        """
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            'SecretString': '{"LLM_API_KEY": "correct-uuid-789"}'
        }
        mock_session.return_value.client.return_value = mock_client

        request = api_rf.get('/test/', HTTP_X_LLM_API_KEY='correct-uuid-789')
        response = mock_llm_view(request)

        assert response.status_code == 200
        assert response.data['success'] is True

    @patch.dict('os.environ', {'LLM_API_SECRET_NAME': 'test-secret-name'})
    @patch('boto3.session.Session')
    def test_valid_uuid_json_format_api_key_allows_access(self, mock_session, api_rf):
        """
        Test that valid UUID (JSON format with 'api_key' key) allows access
        """
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            'SecretString': '{"api_key": "correct-uuid-abc"}'
        }
        mock_session.return_value.client.return_value = mock_client

        request = api_rf.get('/test/', HTTP_X_LLM_API_KEY='correct-uuid-abc')
        response = mock_llm_view(request)

        assert response.status_code == 200
        assert response.data['success'] is True

    # ========== Edge Cases ==========
    @patch.dict('os.environ', {'LLM_API_SECRET_NAME': 'test-secret-name'})
    @patch('boto3.session.Session')
    def test_valid_uuid_with_whitespace_strips_correctly(self, mock_session, api_rf):
        """
        Test that UUID comparison strips whitespace from both header and secret
        """
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            'SecretString': '  correct-uuid-with-spaces  '
        }
        mock_session.return_value.client.return_value = mock_client

        request = api_rf.get('/test/', HTTP_X_LLM_API_KEY='correct-uuid-with-spaces')
        response = mock_llm_view(request)

        assert response.status_code == 200
        assert response.data['success'] is True

    @patch.dict('os.environ', {'LLM_API_SECRET_NAME': 'test-secret-name'})
    @patch('boto3.session.Session')
    def test_post_request_with_valid_uuid(self, mock_session, api_rf):
        """
        Test that POST requests work with valid UUID
        """
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            'SecretString': 'post-uuid-123'
        }
        mock_session.return_value.client.return_value = mock_client

        request = api_rf.post('/test/', {}, HTTP_X_LLM_API_KEY='post-uuid-123')
        response = mock_llm_view(request)

        assert response.status_code == 200
        assert response.data['success'] is True

    @patch.dict('os.environ', {'LLM_API_SECRET_NAME': 'test-secret-name'})
    @patch('boto3.session.Session')
    def test_json_secret_without_valid_key_returns_403(self, mock_session, api_rf):
        """
        Test that JSON secret without uuid, LLM_API_KEY, or api_key returns 403
        """
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            'SecretString': '{"some_other_key": "some-value"}'
        }
        mock_session.return_value.client.return_value = mock_client

        request = api_rf.get('/test/', HTTP_X_LLM_API_KEY='test-uuid')
        response = mock_llm_view(request)

        assert response.status_code == 403
        assert "does not contain a valid UUID key" in response.data['detail']

    @patch.dict('os.environ', {'LLM_API_SECRET_NAME': 'test-secret-name'})
    @patch('boto3.session.Session')
    def test_secret_without_secret_string_returns_403(self, mock_session, api_rf):
        """
        Test that secret without SecretString returns 403
        """
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            'SecretBinary': b'binary-data'
        }
        mock_session.return_value.client.return_value = mock_client

        request = api_rf.get('/test/', HTTP_X_LLM_API_KEY='test-uuid')
        response = mock_llm_view(request)

        assert response.status_code == 403
        assert "secret format is invalid" in response.data['detail']

    @patch.dict('os.environ', {'LLM_API_SECRET_NAME': 'test-secret-name'})
    @patch('boto3.session.Session')
    def test_aws_access_denied_error_returns_403(self, mock_session, api_rf):
        """
        Test that AWS AccessDeniedException returns 403 with appropriate message
        """
        mock_client = MagicMock()
        mock_client.get_secret_value.side_effect = ClientError(
            {'Error': {'Code': 'AccessDeniedException', 'Message': 'Access denied'}},
            'GetSecretValue'
        )
        mock_session.return_value.client.return_value = mock_client

        request = api_rf.get('/test/', HTTP_X_LLM_API_KEY='test-uuid')
        response = mock_llm_view(request)

        assert response.status_code == 403
        assert "Error retrieving LLM API secret" in response.data['detail']
        assert "AccessDeniedException" in response.data['detail']

    @patch.dict('os.environ', {'LLM_API_SECRET_NAME': 'test-secret-name'})
    @patch('boto3.session.Session')
    def test_generic_exception_returns_403(self, mock_session, api_rf):
        """
        Test that unexpected exceptions return 403 with appropriate message
        """
        mock_client = MagicMock()
        mock_client.get_secret_value.side_effect = Exception("Unexpected error occurred")
        mock_session.return_value.client.return_value = mock_client

        request = api_rf.get('/test/', HTTP_X_LLM_API_KEY='test-uuid')
        response = mock_llm_view(request)

        assert response.status_code == 403
        assert "Unexpected error accessing LLM API secret" in response.data['detail']

    @patch.dict('os.environ', {'LLM_API_SECRET_NAME': 'test-secret-name', 'AWS_REGION': 'us-east-1'})
    @patch('boto3.session.Session')
    def test_uses_aws_region_from_environment(self, mock_session, api_rf):
        """
        Test that AWS_REGION environment variable is used when provided
        """
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            'SecretString': 'test-uuid-region'
        }
        mock_session.return_value.client.return_value = mock_client

        request = api_rf.get('/test/', HTTP_X_LLM_API_KEY='test-uuid-region')
        response = mock_llm_view(request)

        # Verify the client was created with the correct region
        mock_session.return_value.client.assert_called_with(
            service_name='secretsmanager',
            region_name='us-east-1'
        )
        assert response.status_code == 200

    @patch.dict('os.environ', {'LLM_API_SECRET_NAME': 'test-secret-name'})
    @patch('boto3.session.Session')
    def test_defaults_to_us_gov_west_1_region(self, mock_session, api_rf):
        """
        Test that AWS region defaults to us-gov-west-1 when AWS_REGION is not set
        """
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            'SecretString': 'test-uuid-default-region'
        }
        mock_session.return_value.client.return_value = mock_client

        request = api_rf.get('/test/', HTTP_X_LLM_API_KEY='test-uuid-default-region')
        response = mock_llm_view(request)

        # Verify the client was created with the default region
        mock_session.return_value.client.assert_called_with(
            service_name='secretsmanager',
            region_name='us-gov-west-1'
        )
        assert response.status_code == 200