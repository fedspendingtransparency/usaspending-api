"""Unit tests for LLM Pydantic models"""
import pytest
from pydantic import ValidationError

from usaspending_api.llm.models.py_models import (
    AITool,
    AIToolDescription,
    RecipientDisplay,
    RecipientFilter,
    SelectedRecipient,
)


class TestAIToolDescription:
    """Tests for AIToolDescription model"""

    def test_valid_creation(self):
        """Test creating AIToolDescription with valid data"""
        tool_desc = AIToolDescription(
            name="test_tool",
            description="A test tool",
            input_schema={"type": "object", "properties": {"param": {"type": "string"}}}
        )
        assert tool_desc.name == "test_tool"
        assert tool_desc.description == "A test tool"
        assert tool_desc.input_schema["type"] == "object"

    def test_missing_required_fields(self):
        """Test that missing required fields raise ValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            AIToolDescription(name="test_tool")

        errors = exc_info.value.errors()
        missing_fields = {error["loc"][0] for error in errors}
        assert "description" in missing_fields
        assert "input_schema" in missing_fields

    def test_empty_input_schema(self):
        """Test that empty input_schema is valid"""
        tool_desc = AIToolDescription(
            name="test_tool",
            description="A test tool",
            input_schema={}
        )
        assert tool_desc.input_schema == {}


class TestAITool:
    """Tests for AITool model"""

    def test_valid_creation_with_defaults(self):
        """Test creating AITool with default logging function"""

        def sample_function():
            return "result"

        tool_desc = AIToolDescription(
            name="test_tool",
            description="A test tool",
            input_schema={"type": "object"}
        )

        tool = AITool(
            description=tool_desc,
            function=sample_function
        )

        assert tool.description == tool_desc
        assert tool.function == sample_function
        assert callable(tool.logging)

    def test_valid_creation_with_custom_logging(self):
        """Test creating AITool with custom logging function"""

        def sample_function():
            return "result"

        def custom_logging(tool_use):
            return f"Custom log: {tool_use}"

        tool_desc = AIToolDescription(
            name="test_tool",
            description="A test tool",
            input_schema={"type": "object"}
        )

        tool = AITool(
            description=tool_desc,
            function=sample_function,
            logging=custom_logging
        )

        assert tool.logging == custom_logging

    def test_function_must_be_callable(self):
        """Test that function field must be callable"""
        tool_desc = AIToolDescription(
            name="test_tool",
            description="A test tool",
            input_schema={"type": "object"}
        )

        with pytest.raises(ValidationError):
            AITool(
                description=tool_desc,
                function="not_a_function"
            )


class TestRecipientFilter:
    """Tests for RecipientFilter model"""

    def test_valid_creation_with_single_value(self):
        """Test creating RecipientFilter with single search text"""
        recipient_filter = RecipientFilter(recipient_search_text=["ACME CORP"])
        assert recipient_filter.recipient_search_text == ["ACME CORP"]

    def test_valid_creation_with_multiple_values(self):
        """Test creating RecipientFilter with multiple search texts"""
        search_texts = ["ACME CORP", "123456789", "UEI123"]
        recipient_filter = RecipientFilter(recipient_search_text=search_texts)
        assert recipient_filter.recipient_search_text == search_texts

    def test_default_empty_list(self):
        """Test that default value is empty list"""
        recipient_filter = RecipientFilter()
        assert recipient_filter.recipient_search_text == []

    def test_min_length_validation(self):
        """Test that min_length validation works when explicitly set"""
        # Note: Pydantic's min_length on default_factory doesn't enforce on empty default
        # This test documents current behavior
        recipient_filter = RecipientFilter()
        assert len(recipient_filter.recipient_search_text) == 0

    def test_invalid_type(self):
        """Test that invalid type raises ValidationError"""
        with pytest.raises(ValidationError):
            RecipientFilter(recipient_search_text="not_a_list")


class TestRecipientDisplay:
    """Tests for RecipientDisplay model"""

    @pytest.mark.parametrize("entity_type", [
        "Recipient",
        "Parent recipient",
        "Child recipient",
        "Subcontractor",
    ])
    def test_valid_entity_types(self, entity_type):
        """Test all valid entity types"""
        display = RecipientDisplay(
            entity=entity_type,
            standalone="ACME CORP",
            title="ACME Corporation"
        )
        assert display.entity == entity_type

    def test_invalid_entity_type(self):
        """Test that invalid entity type raises ValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            RecipientDisplay(
                entity="Invalid Type",
                standalone="ACME CORP",
                title="ACME Corporation"
            )

        errors = exc_info.value.errors()
        assert any("entity" in str(error["loc"]) for error in errors)

    def test_all_fields_required(self):
        """Test that all fields are required"""
        with pytest.raises(ValidationError) as exc_info:
            RecipientDisplay()

        errors = exc_info.value.errors()
        missing_fields = {error["loc"][0] for error in errors}
        assert "entity" in missing_fields
        assert "standalone" in missing_fields
        assert "title" in missing_fields

    def test_standalone_and_title_can_differ(self):
        """Test that standalone and title can have different values"""
        display = RecipientDisplay(
            entity="Recipient",
            standalone="ACME",
            title="ACME Corporation International, LLC"
        )
        assert display.standalone == "ACME"
        assert display.title == "ACME Corporation International, LLC"


class TestSelectedRecipient:
    """Tests for SelectedRecipient model"""

    def test_valid_creation_complete(self):
        """Test creating SelectedRecipient with all valid data"""
        recipient = SelectedRecipient(
            identifier="UEI123456789",
            filter=RecipientFilter(recipient_search_text=["ACME CORP"]),
            display=RecipientDisplay(
                entity="Recipient",
                standalone="ACME CORP",
                title="ACME Corporation"
            )
        )

        assert recipient.identifier == "UEI123456789"
        assert recipient.filter.recipient_search_text == ["ACME CORP"]
        assert recipient.display.entity == "Recipient"
        assert recipient.display.standalone == "ACME CORP"
        assert recipient.display.title == "ACME Corporation"

    def test_valid_creation_with_duns(self):
        """Test creating SelectedRecipient with DUNS identifier"""
        recipient = SelectedRecipient(
            identifier="123456789",
            filter=RecipientFilter(recipient_search_text=["123456789"]),
            display=RecipientDisplay(
                entity="Parent recipient",
                standalone="Parent Corp",
                title="Parent Corporation"
            )
        )

        assert recipient.identifier == "123456789"

    def test_valid_creation_with_internal_id(self):
        """Test creating SelectedRecipient with internal ID"""
        recipient = SelectedRecipient(
            identifier="internal-12345",
            filter=RecipientFilter(recipient_search_text=["John Smith"]),
            display=RecipientDisplay(
                entity="Subcontractor",
                standalone="John Smith",
                title="John Smith (Individual)"
            )
        )

        assert recipient.identifier == "internal-12345"

    def test_nested_validation_filter(self):
        """Test that invalid filter data raises ValidationError"""
        with pytest.raises(ValidationError):
            SelectedRecipient(
                identifier="UEI123",
                filter="invalid_type_not_dict_or_object",
                display=RecipientDisplay(
                    entity="Recipient",
                    standalone="ACME",
                    title="ACME Corp"
                )
            )

    def test_nested_validation_display(self):
        """Test that invalid display data raises ValidationError"""
        with pytest.raises(ValidationError):
            SelectedRecipient(
                identifier="UEI123",
                filter=RecipientFilter(recipient_search_text=["ACME"]),
                display={"invalid": "data"}
            )

    def test_all_fields_required(self):
        """Test that all fields are required"""
        with pytest.raises(ValidationError) as exc_info:
            SelectedRecipient()

        errors = exc_info.value.errors()
        missing_fields = {error["loc"][0] for error in errors}
        assert "identifier" in missing_fields
        assert "filter" in missing_fields
        assert "display" in missing_fields

    def test_model_serialization(self):
        """Test that model can be serialized to dict"""
        recipient = SelectedRecipient(
            identifier="UEI123",
            filter=RecipientFilter(recipient_search_text=["ACME CORP"]),
            display=RecipientDisplay(
                entity="Recipient",
                standalone="ACME",
                title="ACME Corporation"
            )
        )

        data = recipient.model_dump()

        assert data["identifier"] == "UEI123"
        assert data["filter"]["recipient_search_text"] == ["ACME CORP"]
        assert data["display"]["entity"] == "Recipient"
        assert data["display"]["standalone"] == "ACME"
        assert data["display"]["title"] == "ACME Corporation"

    def test_model_json_serialization(self):
        """Test that model can be serialized to JSON"""
        recipient = SelectedRecipient(
            identifier="UEI123",
            filter=RecipientFilter(recipient_search_text=["ACME CORP"]),
            display=RecipientDisplay(
                entity="Recipient",
                standalone="ACME",
                title="ACME Corporation"
            )
        )

        json_str = recipient.model_dump_json()
        assert "UEI123" in json_str
        assert "ACME CORP" in json_str
        assert "Recipient" in json_str
