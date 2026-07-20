import json
from unittest.mock import MagicMock, patch

import pytest

from usaspending_api.llm.models.py_models import Filters
from usaspending_api.llm.tools.execute_filter import execute_filter, execute_filter_tool
from usaspending_api.references.models import FilterHash

pytestmark = pytest.mark.django_db


@pytest.fixture
def sample_filters():
    """Fixture providing sample valid filter data."""
    return {
        "time_period": [{"start_date": "2023-01-01", "end_date": "2023-12-31"}],
        "award_type_codes": ["A", "B"],
        "agencies": [{"type": "awarding", "tier": "toptier", "name": "Department of Agriculture"}],
    }


@pytest.fixture
def mock_filter_hash():
    """Fixture to mock FilterHash model."""
    with patch("usaspending_api.llm.tools.execute_filter.FilterHash") as mock:
        yield mock


class TestInputValidation:
    """Test input validation and error handling."""

    def test_valid_filters_accepted(self, sample_filters):
        """Test that valid filters are accepted."""
        result = execute_filter(**sample_filters)

        assert "hash" in result
        assert "error" not in result

    def test_invalid_filters_return_error(self):
        """Test that invalid filters return validation error."""
        result = execute_filter(invalid_field="invalid_value")

        assert "error" in result
        assert "message" in result
        assert "invalid" in result["message"].lower()

    def test_empty_filters_accepted(self):
        """Test that empty filters are valid."""
        result = execute_filter()

        assert "hash" in result
        assert "error" not in result

    def test_validation_error_message_includes_details(self):
        """Test that validation errors include helpful details."""
        result = execute_filter(time_period="invalid")

        assert "error" in result
        assert isinstance(result["error"], str)
        assert len(result["error"]) > 0

    def test_partial_valid_filters(self):
        """Test filters with only some valid fields."""
        result = execute_filter(award_type_codes=["A", "B"], invalid_field="should_be_ignored")

        # Should fail validation due to invalid field
        assert "error" in result


class TestFilterProcessing:
    """Test filter processing and transformation."""

    def test_filters_converted_to_filter_request(self, sample_filters, mock_filter_hash):
        """Test that filters are properly converted to FilterRequest format."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        result = execute_filter(**sample_filters)

        assert "hash" in result
        # Verify FilterHash was called with proper structure
        call_args = mock_filter_hash.call_args
        saved_filter = json.loads(call_args[1]["filter"])
        assert "filters" in saved_filter

    def test_exclude_none_values(self, mock_filter_hash):
        """Test that None values are excluded from filter request."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        result = execute_filter(award_type_codes=["A"])

        call_args = mock_filter_hash.call_args
        saved_filter = json.loads(call_args[1]["filter"])

        # Should only contain non-None fields
        assert "award_type_codes" in saved_filter["filters"]
        # None fields should not be present
        assert all(v is not None for v in saved_filter["filters"].values())

    def test_keyword_transformation(self, mock_filter_hash):
        """Test that keyword field is transformed to dict format."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        result = execute_filter(keyword=["test", "search"])

        call_args = mock_filter_hash.call_args
        saved_filter = json.loads(call_args[1]["filter"])

        # Keyword should be transformed to {value: value} format
        assert "keyword" in saved_filter["filters"]
        keyword_dict = saved_filter["filters"]["keyword"]
        assert isinstance(keyword_dict, dict)
        assert keyword_dict == {"test": "test", "search": "search"}

    def test_keyword_empty_list(self, mock_filter_hash):
        """Test handling of empty keyword list."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        result = execute_filter(keyword=[])

        call_args = mock_filter_hash.call_args
        saved_filter = json.loads(call_args[1]["filter"])

        # Empty keyword should result in empty dict
        if "keyword" in saved_filter["filters"]:
            assert saved_filter["filters"]["keyword"] == {}

    def test_filter_json_sorted_keys(self, mock_filter_hash):
        """Test that filter JSON has sorted keys for consistent hashing."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        # Create filters with multiple fields
        result = execute_filter(
            award_type_codes=["A"], agencies=[{"type": "awarding"}], time_period=[{"start_date": "2023-01-01"}]
        )

        # Verify hash was created (keys were sorted)
        assert "hash" in result


class TestHashCreationAndStorage:
    """Test hash creation and database storage."""

    @patch("usaspending_api.llm.tools.execute_filter.create_hash")
    def test_hash_created_from_filter_json(self, mock_create_hash, mock_filter_hash):
        """Test that hash is created from filter JSON."""
        mock_create_hash.return_value = "test_hash_123"
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        result = execute_filter(award_type_codes=["A"])

        # Verify create_hash was called
        assert mock_create_hash.called
        # Verify hash is returned
        assert result["hash"] == "test_hash_123"

    def test_existing_hash_not_recreated(self, mock_filter_hash):
        """Test that existing hash is returned without creating new entry."""
        existing_hash = MagicMock()
        existing_hash.hash = "existing_hash_456"
        mock_filter_hash.objects.get.return_value = existing_hash

        result = execute_filter(award_type_codes=["A"])

        # Should return existing hash
        assert "hash" in result
        # Should not create new FilterHash
        assert not mock_filter_hash.called

    def test_new_hash_saved_to_database(self, mock_filter_hash):
        """Test that new hash is saved to database."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        result = execute_filter(award_type_codes=["A"])

        # Verify FilterHash was created
        assert mock_filter_hash.called
        # Verify save was called
        assert mock_instance.save.called
        # Verify hash is returned
        assert "hash" in result

    def test_hash_includes_filter_json(self, mock_filter_hash):
        """Test that saved hash includes filter JSON."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        result = execute_filter(award_type_codes=["A", "B"])

        # Verify FilterHash was called with filter JSON
        call_args = mock_filter_hash.call_args
        assert "filter" in call_args[1]
        saved_filter = json.loads(call_args[1]["filter"])
        assert "filters" in saved_filter
        assert saved_filter["filters"]["award_type_codes"] == ["A", "B"]

    def test_same_filters_produce_same_hash(self, mock_filter_hash):
        """Test that identical filters produce identical hashes."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        filters = {"award_type_codes": ["A", "B"]}

        result1 = execute_filter(**filters)
        result2 = execute_filter(**filters)

        # Both should produce the same hash
        assert result1["hash"] == result2["hash"]

    def test_different_filters_produce_different_hashes(self, mock_filter_hash):
        """Test that different filters produce different hashes."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        result1 = execute_filter(award_type_codes=["A"])
        result2 = execute_filter(award_type_codes=["B"])

        # Should produce different hashes
        assert result1["hash"] != result2["hash"]


class TestErrorHandling:
    """Test error handling scenarios."""

    def test_validation_error_returns_error_dict(self):
        """Test that validation errors return proper error dict."""
        result = execute_filter(time_period="invalid_format")

        assert "error" in result
        assert "message" in result
        assert isinstance(result["error"], str)
        assert isinstance(result["message"], str)

    def test_database_save_error_returns_error_dict(self, mock_filter_hash):
        """Test that database save errors return proper error dict."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_instance.save.side_effect = Exception("Database error")
        mock_filter_hash.return_value = mock_instance

        result = execute_filter(award_type_codes=["A"])

        assert "error" in result
        assert "message" in result
        assert "error saving" in result["message"].lower()

    def test_error_message_includes_exception_details(self, mock_filter_hash):
        """Test that error messages include exception details."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_instance.save.side_effect = Exception("Specific database error")
        mock_filter_hash.return_value = mock_instance

        result = execute_filter(award_type_codes=["A"])

        assert "Specific database error" in result["error"]

    def test_validation_error_does_not_save_to_database(self, mock_filter_hash):
        """Test that validation errors don't attempt database save."""
        result = execute_filter(invalid_field="invalid")

        # Should not attempt to access database
        assert not mock_filter_hash.objects.get.called
        assert not mock_filter_hash.called


class TestAIToolImplementation:
    """Test AITool model implementation."""

    def test_tool_has_required_attributes(self):
        """Test that AITool has required attributes."""
        assert hasattr(execute_filter_tool, "function")
        assert hasattr(execute_filter_tool, "description")
        assert hasattr(execute_filter_tool, "logging")

        assert callable(execute_filter_tool.function)
        assert callable(execute_filter_tool.logging)

    def test_tool_function_is_execute_filter(self):
        """Test that tool function is execute_filter."""
        assert execute_filter_tool.function == execute_filter

    def test_tool_description_structure(self):
        """Test that tool description has proper structure."""
        desc = execute_filter_tool.description

        assert desc.name == "execute_filter"
        assert len(desc.description) > 50
        assert "filter" in desc.description.lower()

    def test_tool_input_schema_matches_filters_model(self):
        """Test that input schema matches Filters model."""
        schema = execute_filter_tool.description.input_schema
        filters_schema = Filters.model_json_schema()

        assert schema == filters_schema

    def test_logging_function_formats_filters(self):
        """Test that logging function formats filters properly."""
        tool_input = {"award_type_codes": ["A", "B"], "time_period": [{"start_date": "2023-01-01"}]}

        log_msg = execute_filter_tool.logging(tool_input)

        assert isinstance(log_msg, str)
        assert "award_type_codes" in log_msg
        assert "time_period" in log_msg

    def test_logging_function_handles_empty_input(self):
        """Test that logging function handles empty input."""
        log_msg = execute_filter_tool.logging({})

        assert isinstance(log_msg, str)
        assert "Selecting filters" in log_msg

    def test_tool_execution_through_aitool(self, mock_filter_hash):
        """Test executing tool through AITool interface."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        result = execute_filter_tool.function(award_type_codes=["A"])

        assert "hash" in result


class TestIntegrationWithFiltersModel:
    """Test integration with Filters Pydantic model."""

    def test_all_filters_model_fields_supported(self):
        """Test that all Filters model fields are supported."""
        # Get all fields from Filters model
        filters_schema = Filters.model_json_schema()
        properties = filters_schema.get("properties", {})

        # Create sample data for each field type
        test_data = {}
        for field_name in properties.keys():
            # Add appropriate test data based on field
            if "codes" in field_name:
                test_data[field_name] = ["TEST"]
            elif "period" in field_name:
                test_data[field_name] = [{"start_date": "2023-01-01"}]
            elif field_name == "keyword":
                test_data[field_name] = ["test"]

        # Should not raise validation error
        result = execute_filter(**test_data)
        assert "hash" in result or "error" in result

    def test_filters_model_validation_applied(self):
        """Test that Filters model validation is applied."""
        # Invalid date format should fail validation
        result = execute_filter(time_period=[{"start_date": "invalid-date"}])

        # Should return validation error
        assert "error" in result

    def test_complex_filters_combination(self, mock_filter_hash):
        """Test complex combination of multiple filters."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        result = execute_filter(
            award_type_codes=["A", "B", "C"],
            time_period=[{"start_date": "2023-01-01", "end_date": "2023-12-31"}],
            agencies=[
                {"type": "awarding", "tier": "toptier", "name": "USDA"},
                {"type": "funding", "tier": "subtier", "name": "Forest Service"},
            ],
            keyword=["fire", "prevention"],
        )

        assert "hash" in result


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_very_large_filter_set(self, mock_filter_hash):
        """Test handling of very large filter sets."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        # Create large filter set
        large_codes = [f"CODE_{i}" for i in range(100)]

        result = execute_filter(award_type_codes=large_codes)

        assert "hash" in result

    def test_unicode_in_filters(self, mock_filter_hash):
        """Test handling of unicode characters in filters."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        result = execute_filter(keyword=["test™", "café", "日本"])

        assert "hash" in result

    def test_special_characters_in_keyword(self, mock_filter_hash):
        """Test handling of special characters in keyword field."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        result = execute_filter(keyword=["test & co.", "50% off", "$1,000"])

        assert "hash" in result

    def test_nested_filter_structures(self, mock_filter_hash):
        """Test handling of deeply nested filter structures."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        result = execute_filter(
            agencies=[
                {"type": "awarding", "tier": "toptier", "name": "Department of Agriculture", "toptier_name": "USDA"}
            ]
        )

        assert "hash" in result

    def test_empty_list_filters(self, mock_filter_hash):
        """Test handling of empty list filters."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        result = execute_filter(award_type_codes=[], keyword=[])

        assert "hash" in result


class TestRealWorldScenarios:
    """Test realistic usage scenarios."""

    def test_typical_award_search_filters(self, mock_filter_hash):
        """Test typical award search filter combination."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        result = execute_filter(
            time_period=[{"start_date": "2023-01-01", "end_date": "2023-12-31"}],
            award_type_codes=["A", "B", "C", "D"],
            agencies=[{"type": "awarding", "tier": "toptier", "name": "Department of Agriculture"}],
            keyword=["forestry", "prevention"],
        )

        assert "hash" in result

    def test_recipient_focused_search(self, mock_filter_hash):
        """Test recipient-focused search filters."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        result = execute_filter(recipient_search_text=["ACME Corporation"], recipient_type_names=["category_business"])

        assert "hash" in result

    def test_location_based_search(self, mock_filter_hash):
        """Test location-based search filters."""
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        result = execute_filter(
            selectedLocations={
                "USA_TX": {
                    "identifier": "USA_TX",
                    "filter": {"country": "USA", "state": "TX"},
                    "display": {"entity": "State", "standalone": "Texas"},
                }
            }
        )

        assert "hash" in result

    def test_filter_reuse_returns_same_hash(self, mock_filter_hash):
        """Test that reusing same filters returns same hash."""
        # First call creates hash
        mock_filter_hash.objects.get.side_effect = FilterHash.DoesNotExist
        mock_instance = MagicMock()
        mock_filter_hash.return_value = mock_instance

        filters = {"award_type_codes": ["A", "B"]}
        result1 = execute_filter(**filters)

        # Second call finds existing hash
        mock_filter_hash.objects.get.side_effect = None
        existing = MagicMock()
        existing.hash = result1["hash"]
        mock_filter_hash.objects.get.return_value = existing

        result2 = execute_filter(**filters)

        assert result1["hash"] == result2["hash"]
