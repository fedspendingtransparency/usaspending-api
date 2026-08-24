"""Unit tests for recipient-related fields in py_models"""
import pytest
from pydantic import ValidationError

from usaspending_api.llm.models.py_models import Filters, InferenceConfig


class TestInferenceConfig:
    """Tests for InferenceConfig Pydantic model"""

    def test_inference_config_with_defaults(self):
        """Test InferenceConfig with default values"""
        config = InferenceConfig()

        assert config.temperature == 0.0
        assert config.topP == 1.0
        assert config.maxTokens == 5000
        assert config.stopSequences == []

    def test_inference_config_with_custom_values(self):
        """Test InferenceConfig with custom values"""
        config = InferenceConfig(temperature=0.7, topP=0.9, maxTokens=8192, stopSequences=["Human:", "User:"])

        assert config.temperature == 0.7
        assert config.topP == 0.9
        assert config.maxTokens == 8192
        assert config.stopSequences == ["Human:", "User:"]

    def test_inference_config_temperature_validation_min(self):
        """Test that temperature must be >= 0.0"""
        with pytest.raises(ValidationError) as exc_info:
            InferenceConfig(temperature=-0.1)

        assert "temperature" in str(exc_info.value)

    def test_inference_config_temperature_validation_max(self):
        """Test that temperature must be <= 1.0"""
        with pytest.raises(ValidationError) as exc_info:
            InferenceConfig(temperature=1.1)

        assert "temperature" in str(exc_info.value)

    def test_inference_config_temperature_boundary_values(self):
        """Test temperature boundary values (0.0 and 1.0)"""
        config_min = InferenceConfig(temperature=0.0)
        config_max = InferenceConfig(temperature=1.0)

        assert config_min.temperature == 0.0
        assert config_max.temperature == 1.0

    def test_inference_config_top_p_validation_min(self):
        """Test that topP must be >= 0.0"""
        with pytest.raises(ValidationError) as exc_info:
            InferenceConfig(topP=-0.1)

        assert "topP" in str(exc_info.value)

    def test_inference_config_top_p_validation_max(self):
        """Test that topP must be <= 1.0"""
        with pytest.raises(ValidationError) as exc_info:
            InferenceConfig(topP=1.1)

        assert "topP" in str(exc_info.value)

    def test_inference_config_top_p_boundary_values(self):
        """Test topP boundary values (0.0 and 1.0)"""
        config_min = InferenceConfig(topP=0.0)
        config_max = InferenceConfig(topP=1.0)

        assert config_min.topP == 0.0
        assert config_max.topP == 1.0

    def test_inference_config_max_tokens_validation(self):
        """Test that maxTokens must be positive"""
        with pytest.raises(ValidationError) as exc_info:
            InferenceConfig(maxTokens=0)

        assert "maxTokens" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            InferenceConfig(maxTokens=-100)

        assert "maxTokens" in str(exc_info.value)

    def test_inference_config_max_tokens_positive_value(self):
        """Test that maxTokens accepts positive values"""
        config = InferenceConfig(maxTokens=1)
        assert config.maxTokens == 1

        config = InferenceConfig(maxTokens=10000)
        assert config.maxTokens == 10000

    def test_inference_config_stop_sequences_empty_list(self):
        """Test stopSequences with empty list"""
        config = InferenceConfig(stopSequences=[])
        assert config.stopSequences == []

    def test_inference_config_stop_sequences_with_values(self):
        """Test stopSequences with multiple values"""
        sequences = ["Human:", "User:", "Assistant:", "\n\n"]
        config = InferenceConfig(stopSequences=sequences)

        assert config.stopSequences == sequences
        assert len(config.stopSequences) == 4

    def test_inference_config_from_dict(self):
        """Test creating InferenceConfig from dict"""
        config_dict = {"temperature": 0.3, "topP": 0.95, "maxTokens": 2048, "stopSequences": ["END"]}

        config = InferenceConfig(**config_dict)

        assert config.temperature == 0.3
        assert config.topP == 0.95
        assert config.maxTokens == 2048
        assert config.stopSequences == ["END"]


class TestRecipientFields:
    """Tests for recipient-related fields in Filters model"""

    def test_selected_recipients_default_empty_list(self):
        """Test that selectedRecipients defaults to empty list"""
        filters = Filters()

        assert filters.selectedRecipients == []
        assert isinstance(filters.selectedRecipients, list)

    def test_selected_recipients_accepts_string_list(self):
        """Test that selectedRecipients accepts list of strings"""
        recipient_ids = ["ACME CORP", "UEI123456789", "123456789"]
        filters = Filters(selectedRecipients=recipient_ids)

        assert filters.selectedRecipients == recipient_ids
        assert len(filters.selectedRecipients) == 3

    def test_selected_recipients_accepts_empty_list(self):
        """Test that selectedRecipients accepts empty list"""
        filters = Filters(selectedRecipients=[])

        assert filters.selectedRecipients == []

    def test_selected_recipients_accepts_single_recipient(self):
        """Test that selectedRecipients works with single recipient"""
        filters = Filters(selectedRecipients=["ACME CORPORATION"])

        assert filters.selectedRecipients == ["ACME CORPORATION"]
        assert len(filters.selectedRecipients) == 1

    def test_selected_recipients_preserves_order(self):
        """Test that selectedRecipients preserves order of recipients"""
        recipients = ["BETA CORP", "ACME CORP", "ZETA INDUSTRIES"]
        filters = Filters(selectedRecipients=recipients)

        assert filters.selectedRecipients == recipients
        assert filters.selectedRecipients[0] == "BETA CORP"
        assert filters.selectedRecipients[2] == "ZETA INDUSTRIES"

    def test_selected_recipients_allows_duplicates(self):
        """Test that selectedRecipients allows duplicate entries"""
        recipients = ["ACME CORP", "ACME CORP", "BETA CORP"]
        filters = Filters(selectedRecipients=recipients)

        assert len(filters.selectedRecipients) == 3
        assert filters.selectedRecipients.count("ACME CORP") == 2

    def test_selected_recipients_rejects_non_string_values(self):
        """Test that selectedRecipients rejects non-string values"""
        with pytest.raises(ValidationError) as exc_info:
            Filters(selectedRecipients=[123, 456])

        assert "selectedRecipients" in str(exc_info.value)

    def test_selected_recipients_rejects_mixed_types(self):
        """Test that selectedRecipients rejects mixed types"""
        with pytest.raises(ValidationError) as exc_info:
            Filters(selectedRecipients=["ACME CORP", 123, None])

        assert "selectedRecipients" in str(exc_info.value)

    def test_selected_recipients_handles_special_characters(self):
        """Test that selectedRecipients handles special characters in names"""
        recipients = [
            "O'REILLY MEDIA",
            "ACME & CO.",
            "BETA CORP (2024)",
            "GAMMA-DELTA LLC",
        ]
        filters = Filters(selectedRecipients=recipients)

        assert filters.selectedRecipients == recipients
        assert "O'REILLY MEDIA" in filters.selectedRecipients

    def test_selected_recipients_handles_unicode(self):
        """Test that selectedRecipients handles unicode characters"""
        recipients = ["Société Générale", "日本株式会社", "Москва ООО"]
        filters = Filters(selectedRecipients=recipients)

        assert filters.selectedRecipients == recipients

    def test_selected_recipients_handles_whitespace(self):
        """Test that selectedRecipients preserves whitespace"""
        recipients = ["  ACME CORP  ", "BETA  CORP", "GAMMA\tCORP"]
        filters = Filters(selectedRecipients=recipients)

        # Whitespace should be preserved as-is
        assert filters.selectedRecipients == recipients


class TestRecipientType:
    """Tests for recipientType field in Filters model"""

    def test_recipient_type_default_empty_list(self):
        """Test that recipientType defaults to empty list"""
        filters = Filters()

        assert filters.recipientType == []
        assert isinstance(filters.recipientType, list)

    def test_recipient_type_accepts_valid_business_types(self):
        """Test that recipientType accepts valid business types"""
        types = ["business", "small_business", "other_than_small_business"]
        filters = Filters(recipientType=types)

        assert filters.recipientType == types
        assert len(filters.recipientType) == 3

    def test_recipient_type_accepts_minority_owned_types(self):
        """Test that recipientType accepts minority-owned business types"""
        types = [
            "minority_owned_business",
            "black_american_owned_business",
            "hispanic_american_owned_business",
        ]
        filters = Filters(recipientType=types)

        assert filters.recipientType == types

    def test_recipient_type_accepts_women_owned_types(self):
        """Test that recipientType accepts women-owned business types"""
        types = [
            "woman_owned_business",
            "women_owned_small_business",
            "economically_disadvantaged_women_owned_small_business",
        ]
        filters = Filters(recipientType=types)

        assert filters.recipientType == types

    def test_recipient_type_accepts_veteran_owned_types(self):
        """Test that recipientType accepts veteran-owned business types"""
        types = [
            "veteran_owned_business",
            "service_disabled_veteran_owned_business",
        ]
        filters = Filters(recipientType=types)

        assert filters.recipientType == types

    def test_recipient_type_accepts_special_designations(self):
        """Test that recipientType accepts special designation types"""
        types = [
            "8a_program_participant",
            "historically_underutilized_business_firm",
            "ability_one_program",
        ]
        filters = Filters(recipientType=types)

        assert filters.recipientType == types

    def test_recipient_type_accepts_nonprofit_types(self):
        """Test that recipientType accepts nonprofit types"""
        types = ["nonprofit", "foundation", "community_development_corporations"]
        filters = Filters(recipientType=types)

        assert filters.recipientType == types

    def test_recipient_type_accepts_higher_education_types(self):
        """Test that recipientType accepts higher education types"""
        types = [
            "higher_education",
            "public_institution_of_higher_education",
            "private_institution_of_higher_education",
            "minority_serving_institution_of_higher_education",
        ]
        filters = Filters(recipientType=types)

        assert filters.recipientType == types

    def test_recipient_type_accepts_government_types(self):
        """Test that recipientType accepts government types"""
        types = [
            "government",
            "national_government",
            "local_government",
            "indian_native_american_tribal_government",
        ]
        filters = Filters(recipientType=types)

        assert filters.recipientType == types

    def test_recipient_type_accepts_individuals(self):
        """Test that recipientType accepts individuals type"""
        filters = Filters(recipientType=["individuals"])

        assert filters.recipientType == ["individuals"]

    def test_recipient_type_accepts_mixed_categories(self):
        """Test that recipientType accepts types from different categories"""
        types = [
            "small_business",
            "woman_owned_business",
            "veteran_owned_business",
            "nonprofit",
            "local_government",
        ]
        filters = Filters(recipientType=types)

        assert filters.recipientType == types
        assert len(filters.recipientType) == 5

    def test_recipient_type_rejects_invalid_type(self):
        """Test that recipientType rejects invalid type values"""
        with pytest.raises(ValidationError) as exc_info:
            Filters(recipientType=["invalid_type"])

        assert "recipientType" in str(exc_info.value)

    def test_recipient_type_rejects_empty_string(self):
        """Test that recipientType rejects empty string"""
        with pytest.raises(ValidationError) as exc_info:
            Filters(recipientType=[""])

        assert "recipientType" in str(exc_info.value)

    def test_recipient_type_rejects_non_string_values(self):
        """Test that recipientType rejects non-string values"""
        with pytest.raises(ValidationError) as exc_info:
            Filters(recipientType=[123, 456])

        assert "recipientType" in str(exc_info.value)

    def test_recipient_type_case_sensitive(self):
        """Test that recipientType is case-sensitive"""
        with pytest.raises(ValidationError) as exc_info:
            Filters(recipientType=["SMALL_BUSINESS"])  # Should be lowercase

        assert "recipientType" in str(exc_info.value)

    def test_recipient_type_allows_duplicates(self):
        """Test that recipientType allows duplicate entries"""
        types = ["small_business", "small_business", "nonprofit"]
        filters = Filters(recipientType=types)

        assert len(filters.recipientType) == 3
        assert filters.recipientType.count("small_business") == 2

    def test_recipient_type_preserves_order(self):
        """Test that recipientType preserves order"""
        types = ["nonprofit", "business", "government"]
        filters = Filters(recipientType=types)

        assert filters.recipientType == types
        assert filters.recipientType[0] == "nonprofit"
        assert filters.recipientType[2] == "government"

    def test_recipient_type_accepts_all_valid_types(self):
        """Test that all documented recipient types are valid"""
        all_types = [
            # Business types
            "business",
            "small_business",
            "other_than_small_business",
            "corporate_entity_tax_exempt",
            "corporate_entity_not_tax_exempt",
            "partnership_or_limited_liability_partnership",
            "sole_proprietorship",
            "manufacturer_of_goods",
            "subchapter_s_corporation",
            "limited_liability_corporation",
            # Minority owned
            "minority_owned_business",
            "alaskan_native_corporation_owned_firm",
            "american_indian_owned_business",
            "asian_pacific_american_owned_business",
            "black_american_owned_business",
            "hispanic_american_owned_business",
            "native_american_owned_business",
            "native_hawaiian_organization_owned_firm",
            "subcontinent_asian_indian_american_owned_business",
            "tribally_owned_firm",
            "other_minority_owned_business",
            # Women owned
            "woman_owned_business",
            "women_owned_small_business",
            "economically_disadvantaged_women_owned_small_business",
            "joint_venture_women_owned_small_business",
            "joint_venture_economically_disadvantaged_women_owned_small_business",
            # Veteran owned
            "veteran_owned_business",
            "service_disabled_veteran_owned_business",
            # Special designations
            "special_designations",
            "8a_program_participant",
            "ability_one_program",
            "dot_certified_disadvantaged_business_enterprise",
            "emerging_small_business",
            "federally_funded_research_and_development_corp",
            "historically_underutilized_business_firm",
            "labor_surplus_area_firm",
            "sba_certified_8a_joint_venture",
            "self_certified_small_disadvanted_business",
            "small_agricultural_cooperative",
            "community_developed_corporation_owned_firm",
            "us_owned_business",
            "foreign_owned_and_us_located_business",
            "foreign_owned",
            "foreign_government",
            "international_organization",
            "domestic_shelter",
            "hospital",
            "veterinary_hospital",
            # Nonprofit
            "nonprofit",
            "foundation",
            "community_development_corporations",
            # Higher education
            "higher_education",
            "public_institution_of_higher_education",
            "private_institution_of_higher_education",
            "minority_serving_institution_of_higher_education",
            "school_of_forestry",
            "veterinary_college",
            # Government
            "government",
            "national_government",
            "interstate_entity",
            "regional_and_state_government",
            "regional_organization",
            "us_territory_or_possession",
            "council_of_governments",
            "local_government",
            "indian_native_american_tribal_government",
            "authorities_and_commissions",
            # Individuals
            "individuals",
        ]

        # Should not raise validation error
        filters = Filters(recipientType=all_types)
        assert len(filters.recipientType) == len(all_types)


class TestRecipientDomesticForeign:
    """Tests for recipientDomesticForeign field in Filters model"""

    def test_recipient_domestic_foreign_default_all(self):
        """Test that recipientDomesticForeign defaults to 'all'"""
        filters = Filters()

        assert filters.recipientDomesticForeign == "all"

    def test_recipient_domestic_foreign_accepts_all(self):
        """Test that recipientDomesticForeign accepts 'all'"""
        filters = Filters(recipientDomesticForeign="all")

        assert filters.recipientDomesticForeign == "all"

    def test_recipient_domestic_foreign_accepts_foreign(self):
        """Test that recipientDomesticForeign accepts 'foreign'"""
        filters = Filters(recipientDomesticForeign="foreign")

        assert filters.recipientDomesticForeign == "foreign"

    def test_recipient_domestic_foreign_rejects_invalid_value(self):
        """Test that recipientDomesticForeign rejects invalid values"""
        with pytest.raises(ValidationError) as exc_info:
            Filters(recipientDomesticForeign="domestic")

        assert "recipientDomesticForeign" in str(exc_info.value)

    def test_recipient_domestic_foreign_rejects_empty_string(self):
        """Test that recipientDomesticForeign rejects empty string"""
        with pytest.raises(ValidationError) as exc_info:
            Filters(recipientDomesticForeign="")

        assert "recipientDomesticForeign" in str(exc_info.value)

    def test_recipient_domestic_foreign_case_sensitive(self):
        """Test that recipientDomesticForeign is case-sensitive"""
        with pytest.raises(ValidationError) as exc_info:
            Filters(recipientDomesticForeign="ALL")

        assert "recipientDomesticForeign" in str(exc_info.value)

    def test_recipient_domestic_foreign_rejects_none(self):
        """Test that recipientDomesticForeign rejects None"""
        with pytest.raises(ValidationError) as exc_info:
            Filters(recipientDomesticForeign=None)

        assert "recipientDomesticForeign" in str(exc_info.value)


class TestRecipientFieldsCombinations:
    """Tests for combinations of recipient-related fields"""

    def test_all_recipient_fields_together(self):
        """Test that all recipient fields can be set together"""
        filters = Filters(
            selectedRecipients=["ACME CORP", "BETA CORP"],
            recipientType=["small_business", "woman_owned_business"],
            recipientDomesticForeign="foreign",
        )

        assert filters.selectedRecipients == ["ACME CORP", "BETA CORP"]
        assert filters.recipientType == ["small_business", "woman_owned_business"]
        assert filters.recipientDomesticForeign == "foreign"

    def test_recipient_fields_with_empty_values(self):
        """Test recipient fields with empty values"""
        filters = Filters(
            selectedRecipients=[],
            recipientType=[],
            recipientDomesticForeign="all",
        )

        assert filters.selectedRecipients == []
        assert filters.recipientType == []
        assert filters.recipientDomesticForeign == "all"

    def test_recipient_fields_independent(self):
        """Test that recipient fields are independent"""
        # Set only selectedRecipients
        filters1 = Filters(selectedRecipients=["ACME CORP"])
        assert filters1.selectedRecipients == ["ACME CORP"]
        assert filters1.recipientType == []
        assert filters1.recipientDomesticForeign == "all"

        # Set only recipientType
        filters2 = Filters(recipientType=["small_business"])
        assert filters2.selectedRecipients == []
        assert filters2.recipientType == ["small_business"]
        assert filters2.recipientDomesticForeign == "all"

        # Set only recipientDomesticForeign
        filters3 = Filters(recipientDomesticForeign="foreign")
        assert filters3.selectedRecipients == []
        assert filters3.recipientType == []
        assert filters3.recipientDomesticForeign == "foreign"

    def test_recipient_fields_serialization(self):
        """Test that recipient fields serialize correctly"""
        filters = Filters(
            selectedRecipients=["ACME CORP"],
            recipientType=["small_business", "nonprofit"],
            recipientDomesticForeign="foreign",
        )

        data = filters.model_dump()

        assert data["selectedRecipients"] == ["ACME CORP"]
        assert data["recipientType"] == ["small_business", "nonprofit"]
        assert data["recipientDomesticForeign"] == "foreign"

    def test_recipient_fields_deserialization(self):
        """Test that recipient fields deserialize correctly"""
        data = {
            "selectedRecipients": ["BETA CORP", "GAMMA CORP"],
            "recipientType": ["veteran_owned_business", "8a_program_participant"],
            "recipientDomesticForeign": "all",
        }

        filters = Filters(**data)

        assert filters.selectedRecipients == ["BETA CORP", "GAMMA CORP"]
        assert filters.recipientType == ["veteran_owned_business", "8a_program_participant"]
        assert filters.recipientDomesticForeign == "all"

    def test_recipient_fields_json_schema(self):
        """Test that recipient fields have proper JSON schema"""
        schema = Filters.model_json_schema()

        # Check selectedRecipients schema
        assert "selectedRecipients" in schema["properties"]
        assert schema["properties"]["selectedRecipients"]["type"] == "array"
        assert schema["properties"]["selectedRecipients"]["items"]["type"] == "string"

        # Check recipientType schema
        assert "recipientType" in schema["properties"]
        assert schema["properties"]["recipientType"]["type"] == "array"

        # Check recipientDomesticForeign schema
        assert "recipientDomesticForeign" in schema["properties"]
        assert "enum" in schema["properties"]["recipientDomesticForeign"]
        assert set(schema["properties"]["recipientDomesticForeign"]["enum"]) == {"all", "foreign"}
