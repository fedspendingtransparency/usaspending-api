import json
from unittest.mock import Mock, patch

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.llm.models.db_models import Assistant, Session


@pytest.fixture
def ai_model_data(db):
    """Create AI model test data."""
    ai_model = baker.make(
        "llm.AIModel",
        name="nova micro",
        model_id="amazon.nova-micro-v1:0",
        provider="amazon",
    )
    Assistant.objects.create(name="filter-search", ai_model=ai_model, is_active=True)
    return ai_model


@pytest.fixture
def mock_bedrock_client():
    """Mock boto3 bedrock client."""
    with patch("boto3.client") as mock_client:
        mock_instance = Mock()
        mock_client.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_llm_api_key():
    """Mock LLM API key validation."""
    with patch("usaspending_api.common.api_request_utils.LLMAPIKeyHandler._validate_llm_request") as mock_validate:
        mock_validate.return_value = None  # None means validation passed.
        yield mock_validate


@pytest.fixture
def system_prompt_data(db):
    """Create system prompt test data."""
    return baker.make(
        "llm.Prompts",
        name="initial",
        description="initial system prompt for the search assistant",
        text="You are USAspending search assistant. You help the user search for federal spending.",
    )


class TestFilterSearch:
    """Integration tests for /api/v2/llm/filter-search/ API endpoint."""

    url = "/api/v2/llm/filter-search/"

    def test_endpoint_requires_api_key(self, client, ai_model_data):
        """Test that endpoint requires X-LLM-API-Key header."""
        resp = client.post(self.url, content_type="application/json", data=json.dumps({"query": "test query"}))
        # Without mocking the API key validator, it should fail.
        # The actual status depends on whether the secret is configured.
        assert resp.status_code in [status.HTTP_403_FORBIDDEN, status.HTTP_500_INTERNAL_SERVER_ERROR]

    @pytest.mark.django_db
    def test_endpoint_rejects_missing_query(self, client, ai_model_data, mock_llm_api_key):
        """Test that endpoint rejects requests without query parameter."""
        resp = client.post(self.url, content_type="application/json", data=json.dumps({}))
        # Endpoint returns streaming response even for validation errors.
        assert resp.status_code == status.HTTP_200_OK
        assert resp["Content-Type"] == "application/x-ndjson"

        # Parse streaming response.
        content = b"".join(resp.streaming_content).decode("utf-8")
        lines = [line for line in content.strip().split("\n") if line]
        event = json.loads(lines[0])

        assert event["type"] == "search_error"
        assert "query" in event["message"].lower()

    @pytest.mark.django_db
    def test_endpoint_rejects_empty_query(self, client, ai_model_data, mock_llm_api_key):
        """Test that endpoint rejects empty query string."""
        resp = client.post(self.url, content_type="application/json", data=json.dumps({"query": ""}))
        # Endpoint returns streaming response even for validation errors.
        assert resp.status_code == status.HTTP_200_OK
        assert resp["Content-Type"] == "application/x-ndjson"

        # Parse streaming response.
        content = b"".join(resp.streaming_content).decode("utf-8")
        lines = [line for line in content.strip().split("\n") if line]
        event = json.loads(lines[0])

        assert event["type"] == "search_error"
        assert "query" in event["message"].lower() or "min" in event["message"].lower()

    @pytest.mark.django_db
    def test_endpoint_rejects_query_too_long(self, client, ai_model_data, mock_llm_api_key):
        """Test that endpoint rejects query strings longer than 1000 characters."""
        long_query = "a" * 1001
        resp = client.post(self.url, content_type="application/json", data=json.dumps({"query": long_query}))
        # Endpoint returns streaming response even for validation errors.
        assert resp.status_code == status.HTTP_200_OK
        assert resp["Content-Type"] == "application/x-ndjson"

        # Parse streaming response.
        content = b"".join(resp.streaming_content).decode("utf-8")
        lines = [line for line in content.strip().split("\n") if line]
        event = json.loads(lines[0])

        assert event["type"] == "search_error"
        assert "max" in event["message"].lower() or "1000" in event["message"]
        # NOTE: The backend limitation on queries should be unnecessary if client-side restricts input length,
        #       but it's good to have in case users find ways to bypass client-side restrictions.

    def test_endpoint_accepts_valid_query(self, client, ai_model_data, mock_llm_api_key, mock_bedrock_client):
        """Test that endpoint accepts valid query and returns streaming response."""
        # Mock Bedrock response.
        mock_bedrock_client.converse.return_value = {
            "output": {"message": {"role": "assistant", "content": [{"text": "Here are your results"}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 20},
            "metrics": {"latencyMs": 100},
        }

        resp = client.post(
            self.url, content_type="application/json", data=json.dumps({"query": "Find contracts in California"})
        )

        assert resp.status_code == status.HTTP_200_OK
        assert resp["Content-Type"] == "application/x-ndjson"
        assert resp["Cache-Control"] == "no-cache"
        assert resp["X-Accel-Buffering"] == "no"

    def test_endpoint_creates_session(self, client, ai_model_data, mock_llm_api_key, mock_bedrock_client):
        """Test that endpoint creates a Session record."""
        # Mock Bedrock response.
        mock_bedrock_client.converse.return_value = {
            "output": {"message": {"role": "assistant", "content": [{"text": "Results"}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 20},
            "metrics": {"latencyMs": 100},
        }

        initial_session_count = Session.objects.count()

        resp = client.post(self.url, content_type="application/json", data=json.dumps({"query": "test query"}))

        assert resp.status_code == status.HTTP_200_OK
        assert Session.objects.count() == initial_session_count + 1

        # Verify session has correct attributes.
        session = Session.objects.latest("started_at")
        assert session.ai_model == ai_model_data
        assert "lookup_location" in session.tools
        assert "lookup_recipient" in session.tools

    def test_endpoint_returns_ndjson_stream(self, client, ai_model_data, mock_llm_api_key, mock_bedrock_client):
        """Test that endpoint returns properly formatted NDJSON stream."""
        # Mock Bedrock response.
        mock_bedrock_client.converse.return_value = {
            "output": {"message": {"role": "assistant", "content": [{"text": "Results"}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 20},
            "metrics": {"latencyMs": 100},
        }

        resp = client.post(self.url, content_type="application/json", data=json.dumps({"query": "test query"}))

        assert resp.status_code == status.HTTP_200_OK

        # Parse NDJSON response.
        content = b"".join(resp.streaming_content).decode("utf-8")
        lines = [line for line in content.strip().split("\n") if line]

        # Should have at least search_start event.
        assert len(lines) >= 1

        # Parse first event.
        first_event = json.loads(lines[0])
        assert "search_id" in first_event
        assert "type" in first_event
        assert first_event["type"] == "search_start"

    def test_endpoint_with_tool_execution(self, client, ai_model_data, mock_llm_api_key, mock_bedrock_client):
        """Test endpoint with tool execution in the response."""
        # Mock Bedrock to return tool_use.
        mock_bedrock_client.converse.side_effect = [
            # First call: LLM requests tool use.
            {
                "output": {
                    "message": {
                        "role": "assistant",
                        "content": [
                            {
                                "toolUse": {
                                    "toolUseId": "tool-123",
                                    "name": "lookup_location",
                                    "input": {"query": "California"},
                                }
                            }
                        ],
                    }
                },
                "stopReason": "tool_use",
                "usage": {"inputTokens": 10, "outputTokens": 20},
                "metrics": {"latencyMs": 100},
            },
            # Second call: LLM provides final response.
            {
                "output": {"message": {"role": "assistant", "content": [{"text": "Found California"}]}},
                "stopReason": "end_turn",
                "usage": {"inputTokens": 15, "outputTokens": 25},
                "metrics": {"latencyMs": 120},
            },
        ]

        resp = client.post(
            self.url, content_type="application/json", data=json.dumps({"query": "Find contracts in California"})
        )

        assert resp.status_code == status.HTTP_200_OK

        # Parse NDJSON response.
        content = b"".join(resp.streaming_content).decode("utf-8")
        lines = [line for line in content.strip().split("\n") if line]
        events = [json.loads(line) for line in lines]

        # Should have: search_start, tool_start, tool_complete.
        event_types = [e["type"] for e in events]
        assert "search_start" in event_types
        assert "tool_start" in event_types
        assert "tool_complete" in event_types

        # Verify tool events have tool_use_id.
        tool_events = [e for e in events if e["type"] in ["tool_start", "tool_complete"]]
        for tool_event in tool_events:
            assert "tool_use_id" in tool_event

    @pytest.mark.django_db
    def test_endpoint_handles_missing_ai_model(self, client, mock_llm_api_key):
        """Test that endpoint handles missing AI model gracefully."""
        # Don't create ai_model_data fixture.
        resp = client.post(self.url, content_type="application/json", data=json.dumps({"query": "test query"}))

        assert resp.status_code == status.HTTP_200_OK
        assert resp["Content-Type"] == "application/x-ndjson"

        # Parse response.
        content = b"".join(resp.streaming_content).decode("utf-8")
        lines = [line for line in content.strip().split("\n") if line]
        event = json.loads(lines[0])

        assert event["type"] == "search_error"
        assert "not found" in event["message"].lower()

    @pytest.mark.django_db
    def test_endpoint_uses_active_filter_search_assistant(
        self, client, mock_llm_api_key, mock_bedrock_client, system_prompt_data
    ):
        """Test that endpoint uses the active filter-search Assistant configuration."""
        custom_model = baker.make(
            "llm.AIModel",
            name="claude 4.5",
            model_id="anthropic.claude-sonnet-4-5-20250929-v1:0",
            provider="anthropic",
        )
        Assistant.objects.create(
            name="filter-search",
            ai_model=custom_model,
            system_prompt=system_prompt_data,
            inference_config={"temperature": 0.4},
            is_active=True,
        )

        mock_bedrock_client.converse.return_value = {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [{"text": "Results"}],
                }
            },
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 20},
            "metrics": {"latencyMs": 100},
        }

        resp = client.post(
            self.url,
            content_type="application/json",
            data=json.dumps({"query": "test query"}),
        )

        assert resp.status_code == status.HTTP_200_OK
        list(resp.streaming_content)

        session = Session.objects.latest("started_at")
        assert session.ai_model == custom_model
        assert session.system_prompt == system_prompt_data
        assert mock_bedrock_client.converse.call_args.kwargs["inferenceConfig"] == {"temperature": 0.4}
        assert mock_bedrock_client.converse.call_args.kwargs["system"] == [{"text": system_prompt_data.text}]

    def test_endpoint_handles_bedrock_error(self, client, ai_model_data, mock_llm_api_key, mock_bedrock_client):
        """Test that endpoint handles Bedrock API errors gracefully."""
        # Mock Bedrock to raise an error.
        mock_bedrock_client.converse.side_effect = Exception("Bedrock API error")

        resp = client.post(self.url, content_type="application/json", data=json.dumps({"query": "test query"}))

        assert resp.status_code == status.HTTP_200_OK

        # Parse response.
        content = b"".join(resp.streaming_content).decode("utf-8")
        lines = [line for line in content.strip().split("\n") if line]

        # Should have search_start and search_error.
        events = [json.loads(line) for line in lines]
        event_types = [e["type"] for e in events]
        assert "search_start" in event_types
        assert "search_error" in event_types

        # Verify error message.
        error_event = next(e for e in events if e["type"] == "search_error")
        assert "error" in error_event["message"].lower()

    def test_endpoint_validates_query_length_boundaries(
        self, client, ai_model_data, mock_llm_api_key, mock_bedrock_client
    ):
        """Test query length validation at boundaries."""
        # Mock Bedrock response.
        mock_bedrock_client.converse.return_value = {
            "output": {"message": {"role": "assistant", "content": [{"text": "Results"}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 20},
            "metrics": {"latencyMs": 100},
        }

        # Test minimum length (1 character).
        resp = client.post(self.url, content_type="application/json", data=json.dumps({"query": "a"}))
        assert resp.status_code == status.HTTP_200_OK

        # Test maximum length (1000 characters).
        max_query = "a" * 1000
        resp = client.post(self.url, content_type="application/json", data=json.dumps({"query": max_query}))
        assert resp.status_code == status.HTTP_200_OK

    @pytest.mark.django_db
    def test_endpoint_search_id_consistency(
        self, client, ai_model_data, mock_llm_api_key, mock_bedrock_client, system_prompt_data
    ):
        """Test that all events in a stream share the same search_id (as strings)."""
        # Mock Bedrock with tool use.
        mock_bedrock_client.converse.side_effect = [
            {
                "output": {
                    "message": {
                        "role": "assistant",
                        "content": [
                            {
                                "toolUse": {
                                    "toolUseId": "tool-123",
                                    "name": "lookup_location",
                                    "input": {"query": "Texas"},
                                }
                            }
                        ],
                    }
                },
                "stopReason": "tool_use",
                "usage": {"inputTokens": 10, "outputTokens": 20},
                "metrics": {"latencyMs": 100},
            },
            {
                "output": {"message": {"role": "assistant", "content": [{"text": "Done"}]}},
                "stopReason": "end_turn",
                "usage": {"inputTokens": 15, "outputTokens": 25},
                "metrics": {"latencyMs": 120},
            },
        ]

        resp = client.post(self.url, content_type="application/json", data=json.dumps({"query": "test query"}))

        # Parse all events.
        content = b"".join(resp.streaming_content).decode("utf-8")
        lines = [line for line in content.strip().split("\n") if line]
        events = [json.loads(line) for line in lines]

        # All events should have the same search_id (as strings).
        search_ids = [e["search_id"] for e in events]
        assert len(set(search_ids)) == 1, "All events should share the same search_id"
        # Verify all search_ids are strings (not mixed types).
        assert all(isinstance(sid, str) for sid in search_ids), "All search_ids should be strings"
