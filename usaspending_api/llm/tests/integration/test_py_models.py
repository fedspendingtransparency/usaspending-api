"""Unit tests for recipient-related fields in py_models"""

import pytest
from model_bakery import baker
from pydantic import ValidationError

from usaspending_api.llm.models.py_models import (
    AWARD_TYPE_LABELS,
    AwardAmounts,
    ExecuteFilterInput,
    Filters,
    InferenceConfig,
    NaicsCodeLists,
    PscCodeLists,
    SelectedAgency,
    SelectedCFDA,
    TasCodeLists,
    clear_defc_rows_cache,
)


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

    def test_inference_config_accepts_none_for_temperature(self):
        """Test that temperature can be None to use model defaults"""
        config = InferenceConfig(temperature=None)
        assert config.temperature is None

    def test_inference_config_accepts_none_for_top_p(self):
        """Test that topP can be None to use model defaults"""
        config = InferenceConfig(topP=None)
        assert config.topP is None

    def test_inference_config_accepts_none_for_max_tokens(self):
        """Test that maxTokens can be None to use model defaults"""
        config = InferenceConfig(maxTokens=None)
        assert config.maxTokens is None

    def test_inference_config_accepts_none_for_stop_sequences(self):
        """Test that stopSequences can be None to use model defaults"""
        config = InferenceConfig(stopSequences=None)
        assert config.stopSequences is None

    def test_inference_config_all_none_values(self):
        """Test that all fields can be None simultaneously"""
        config = InferenceConfig(temperature=None, topP=None, maxTokens=None, stopSequences=None)
        assert config.temperature is None
        assert config.topP is None
        assert config.maxTokens is None
        assert config.stopSequences is None

    def test_inference_config_mixed_none_and_values(self):
        """Test mixing None and actual values"""
        config = InferenceConfig(temperature=0.7, topP=None, maxTokens=1000, stopSequences=None)
        assert config.temperature == 0.7
        assert config.topP is None
        assert config.maxTokens == 1000
        assert config.stopSequences is None


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


class TestSelectedAgencyRekeying:
    """Tests for the "{id}_{agencyType}" re-keying of selected agency dicts on the Filters model"""

    TOPTIER_AGENCY = {
        "id": 1173,
        "agencyType": "toptier",
        "toptier_flag": True,
        "toptier_agency": {
            "id": 126,
            "toptier_code": "097",
            "abbreviation": "DOD",
            "name": "Department of Defense",
        },
    }

    def test_rekeys_empty_key_from_value(self):
        filters = Filters(selectedAwardingAgencies={"": dict(self.TOPTIER_AGENCY)})

        assert list(filters.selectedAwardingAgencies.keys()) == ["1173_toptier"]

    def test_rekeys_wrong_key_from_value(self):
        filters = Filters(selectedFundingAgencies={"wrong-key": dict(self.TOPTIER_AGENCY)})

        assert list(filters.selectedFundingAgencies.keys()) == ["1173_toptier"]

    def test_preserves_correct_key(self):
        filters = Filters(selectedAwardingAgencies={"1173_toptier": dict(self.TOPTIER_AGENCY)})

        assert list(filters.selectedAwardingAgencies.keys()) == ["1173_toptier"]

    def test_subtier_key_uses_subtier_agency_type(self):
        subtier = {
            "id": 42,
            "agencyType": "subtier",
            "toptier_flag": False,
            "toptier_agency": {
                "id": 126,
                "toptier_code": "097",
                "abbreviation": "DOD",
                "name": "Department of Defense",
            },
            "subtier_agency": {"abbreviation": "USA", "name": "Department of the Army"},
        }
        filters = Filters(selectedAwardingAgencies={"": subtier})

        assert list(filters.selectedAwardingAgencies.keys()) == ["42_subtier"]

    def test_empty_dict_stays_empty(self):
        filters = Filters(selectedAwardingAgencies={})

        assert filters.selectedAwardingAgencies == {}


class TestSelectedAgencySubtierDefaulting:
    """Tests for the subtier_agency defaulting/validation on the SelectedAgency model.

    The frontend reads subtier_agency.name/.abbreviation unconditionally, so a toptier-only selection must mirror
    the toptier agency's own name/abbreviation into subtier_agency rather than leaving it None.
    """

    TOPTIER_AGENCY = {
        "id": 126,
        "toptier_code": "097",
        "abbreviation": "DOD",
        "name": "Department of Defense",
    }

    def test_toptier_selection_defaults_subtier_agency_to_toptier(self):
        agency = SelectedAgency(
            id=1173,
            agencyType="toptier",
            toptier_flag=True,
            toptier_agency=self.TOPTIER_AGENCY,
        )

        assert agency.subtier_agency is not None
        assert agency.subtier_agency.abbreviation == "DOD"
        assert agency.subtier_agency.name == "Department of Defense"

    def test_toptier_agency_without_abbreviation_defaults_to_empty_string(self):
        """Some toptier agencies have a NULL abbreviation in the DB; lookup_agencies must not crash on them."""
        agency = SelectedAgency(
            id=1173,
            agencyType="toptier",
            toptier_flag=True,
            toptier_agency={**self.TOPTIER_AGENCY, "abbreviation": None},
        )

        assert agency.toptier_agency.abbreviation == ""
        assert agency.subtier_agency.abbreviation == ""

    def test_subtier_selection_keeps_its_own_subtier_agency(self):
        agency = SelectedAgency(
            id=42,
            agencyType="subtier",
            toptier_flag=False,
            toptier_agency=self.TOPTIER_AGENCY,
            subtier_agency={"abbreviation": "USA", "name": "Department of the Army"},
        )

        assert agency.subtier_agency.abbreviation == "USA"
        assert agency.subtier_agency.name == "Department of the Army"

    def test_subtier_selection_without_subtier_agency_is_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            SelectedAgency(
                id=42,
                agencyType="subtier",
                toptier_flag=False,
                toptier_agency=self.TOPTIER_AGENCY,
            )

        assert "subtier_agency is required when agencyType='subtier'" in str(exc_info.value)

    def test_serialized_toptier_selection_always_includes_subtier_agency(self):
        """exclude_none=True (used when persisting to FilterHash) must not drop subtier_agency."""
        agency = SelectedAgency(
            id=1173,
            agencyType="toptier",
            toptier_flag=True,
            toptier_agency=self.TOPTIER_AGENCY,
        )

        dumped = agency.model_dump(exclude_none=True)

        assert dumped["subtier_agency"] == {"abbreviation": "DOD", "name": "Department of Defense"}


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


class TestTimePeriod:
    """Tests for the start_date/end_date format validation on the TimePeriod model"""

    def test_time_period_accepts_valid_dates(self):
        filters = Filters(timePeriodType="dr", time_period=[{"start_date": "2023-01-15", "end_date": "2023-12-31"}])

        assert filters.time_period[0].start_date == "2023-01-15"
        assert filters.time_period[0].end_date == "2023-12-31"

    def test_time_period_rejects_unparseable_start_date(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(timePeriodType="dr", time_period=[{"start_date": "not-a-date", "end_date": "2023-12-31"}])

        assert "not a valid date" in str(exc_info.value)

    def test_time_period_rejects_unparseable_end_date(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(timePeriodType="dr", time_period=[{"start_date": "2023-01-15", "end_date": "12/31/2023"}])

        assert "not a valid date" in str(exc_info.value)

    def test_time_period_rejects_malformed_but_numeric_date(self):
        """Dates must be real calendar dates, not just digit-shaped strings."""
        with pytest.raises(ValidationError) as exc_info:
            Filters(timePeriodType="dr", time_period=[{"start_date": "2023-13-01", "end_date": "2023-12-31"}])

        assert "not a valid date" in str(exc_info.value)


class TestTimePeriodFY:
    """Tests for the timePeriodFY four-digit-year validation on the Filters model"""

    def test_time_period_fy_default_empty_list(self):
        filters = Filters()

        assert filters.timePeriodFY == []

    def test_time_period_fy_accepts_four_digit_years(self):
        filters = Filters(timePeriodFY=["2023", "2024"])

        assert filters.timePeriodFY == ["2023", "2024"]

    def test_time_period_fy_accepts_single_year(self):
        filters = Filters(timePeriodFY=["2026"])

        assert filters.timePeriodFY == ["2026"]

    def test_time_period_fy_rejects_fy_prefix(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(timePeriodFY=["FY2026"])

        assert "Invalid fiscal year(s)" in str(exc_info.value)

    def test_time_period_fy_rejects_fy_prefix_mixed_with_valid_year(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(timePeriodFY=["2024", "FY2026"])

        assert "FY2026" in str(exc_info.value)

    def test_time_period_fy_rejects_non_four_digit_year(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(timePeriodFY=["26"])

        assert "Invalid fiscal year(s)" in str(exc_info.value)

    def test_time_period_fy_rejects_non_numeric_value(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(timePeriodFY=["this year"])

        assert "Invalid fiscal year(s)" in str(exc_info.value)

    @pytest.mark.parametrize("raw", ["&quot;2025&quot;", '"2025"', "'2025'", " 2025 ", "&#39;2025&#39;"])
    def test_time_period_fy_strips_quotes_and_html_entities(self, raw):
        """The LLM can't see that it is HTML-escaping quotes around years; clean that up instead of erroring."""
        assert Filters(timePeriodFY=[raw]).timePeriodFY == ["2025"]


class TestSetAside:
    def test_valid_codes_accepted(self):
        assert Filters(setAside=["BI", "IEE", "ISBEE"]).setAside == ["BI", "IEE", "ISBEE"]

    def test_invalid_code_error_names_the_valid_codes(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(setAside=["NOT_A_CODE"])

        message = str(exc_info.value)
        assert "Invalid setAside code(s): ['NOT_A_CODE']" in message
        assert "BI = Buy Indian" in message
        assert "ISBEE = Indian Small Business Economic Enterprise" in message

    def test_recipient_type_value_points_to_recipient_type(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(setAside=["native_american_owned_business"])

        assert "are recipientType values, not set-aside codes" in str(exc_info.value)


class TestAwardType:
    """awardType is selected by frontend name and persisted as legacy-expanded codes on the Filters model."""

    def test_award_type_default_empty_list(self):
        filters = Filters()

        assert filters.awardType == []
        assert isinstance(filters.awardType, list)

    def test_award_type_empty_list_is_valid(self):
        assert Filters(awardType=[]).awardType == []

    @pytest.mark.parametrize(
        "label, expected",
        [
            ("All Award Types", []),
            (
                "Financial Assistance",
                ["02", "03", "04", "05", "06", "07", "08", "10", "F001", "F002", "F003", "F004", "F006", "F007"],
            ),
            ("Contracts", ["A", "B", "C", "D"]),
            ("Contract IDVs", ["IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"]),
            ("Grants", ["02", "03", "04", "F001"]),
            ("Cooperative Agreement", ["05", "F002"]),
            ("Direct Payments", ["06", "10", "F006", "F007"]),
            ("Loans", ["07", "08", "F003", "F004"]),
            ("Other", ["-1", "09", "11", "F005", "F008", "F009", "F010"]),
            ("Blanket Purchase Agreements (BPA) Calls", ["A"]),
            ("Purchase Orders (PO)", ["B"]),
            ("Delivery Orders (DO)", ["C"]),
            ("Definitive Contracts", ["D"]),
            ("Government-Wide Acquisition Contract (GWAC)", ["IDV_A"]),
            ("Indefinite Delivery / Requirements Contract", ["IDV_B_A"]),
            ("Indefinite Delivery / Indefinite Quantity (IDIQ) Contract", ["IDV_B_B"]),
            ("Indefinite Delivery / Definite Quantity Contract", ["IDV_B_C"]),
            ("Federal Supply Schedule (FSS)", ["IDV_C"]),
            ("Basic Ordering Agreement (BOA)", ["IDV_D"]),
            ("Blanket Purchase Agreements (BPA)", ["IDV_E"]),
            ("Direct Payment with Unrestricted Use", ["10", "F007"]),
            ("Direct Payment for Specified Use", ["06", "F006"]),
            ("Direct Loans", ["07", "F003"]),
            ("Loan Guarantees", ["08", "F004"]),
            ("Indemnity/Insurance (non-loan)", ["09", "F005"]),
            ("Asset Forfeiture/Equitable Sharing", ["F008"]),
            ("Sale, Exchange, or Donation of Property and Goods", ["F009"]),
            ("Other Financial Assistance", ["11", "F010"]),
            ("Not Specified", ["-1"]),
        ],
    )
    def test_award_type_label_resolves_to_codes(self, label, expected):
        assert Filters(awardType=[label]).awardType == expected

    def test_every_label_is_covered_by_a_test(self):
        """Fails when a label is added to AWARD_TYPE_LABELS without a resolution test above."""
        assert len(AWARD_TYPE_LABELS) == 29

    def test_label_matching_ignores_case_and_extra_whitespace(self):
        assert Filters(awardType=["  direct   LOANS "]).awardType == ["07", "F003"]

    def test_multiple_groups_can_be_combined(self):
        assert Filters(awardType=["Contracts", "Loans"]).awardType == ["07", "08", "A", "B", "C", "D", "F003", "F004"]

    def test_grants_does_not_include_cooperative_agreements(self):
        assert "05" not in Filters(awardType=["Grants"]).awardType
        assert "F002" not in Filters(awardType=["Grants"]).awardType

    def test_all_award_types_overrides_other_selections(self):
        assert Filters(awardType=["Contracts", "All Award Types"]).awardType == []

    def test_financial_assistance_excludes_contracts_idvs_and_other(self):
        codes = Filters(awardType=["Financial Assistance"]).awardType

        assert not {"A", "B", "C", "D"} & set(codes)
        assert not any(code.startswith("IDV_") for code in codes)
        assert not {"-1", "09", "11", "F005", "F008", "F009", "F010"} & set(codes)

    def test_financial_assistance_plus_remaining_groups_means_no_filter(self):
        assert Filters(awardType=["Financial Assistance", "Contracts", "Contract IDVs", "Other"]).awardType == []

    def test_selecting_every_group_means_no_award_type_filter(self):
        every_group = [
            "Contracts",
            "Contract IDVs",
            "Grants",
            "Cooperative Agreement",
            "Direct Payments",
            "Loans",
            "Other",
        ]

        assert Filters(awardType=every_group).awardType == []

    def test_selecting_all_but_one_group_keeps_the_filter(self):
        """Grants and Cooperative Agreement share a backend category, so leaving one out must not collapse."""
        filters = Filters(awardType=["Contracts", "Contract IDVs", "Grants", "Direct Payments", "Loans", "Other"])

        assert filters.awardType != []
        assert "F002" not in filters.awardType

    def test_legacy_codes_resolve_like_current_codes(self):
        assert Filters(awardType=["07"]).awardType == Filters(awardType=["F003"]).awardType == ["07", "F003"]

    def test_raw_contract_codes_pass_through(self):
        assert Filters(awardType=["A", "B"]).awardType == ["A", "B"]

    def test_names_and_codes_can_be_mixed(self):
        assert Filters(awardType=["Grants", "F002"]).awardType == ["02", "03", "04", "05", "F001", "F002"]

    def test_unknown_award_type_rejected_with_guidance(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(awardType=["Contracts", "NOT_A_REAL_TYPE"])

        message = str(exc_info.value)
        assert "Unknown awardType value(s): ['NOT_A_REAL_TYPE']" in message
        assert "omit awardType" in message

    def test_execute_filter_schema_offers_only_names(self):
        schema = ExecuteFilterInput.model_json_schema()
        options = schema["properties"]["awardType"]["items"]["enum"]

        assert options == list(AWARD_TYPE_LABELS)
        assert "F001" not in options and "A" not in options


@pytest.mark.django_db
class TestDefCodes:
    """Tests for the DEFC code restriction (COVID-19 + Infrastructure only) on the Filters model.

    DEFC validity is sourced from the DisasterEmergencyFundCode table (see get_defc_rows), cached
    for the life of the process -- clear_defc_rows_cache() must run before each test so one test's
    DB-seeded codes don't leak into another's (django_db rolls back the DB, but not the Python-level cache).
    """

    @pytest.fixture(autouse=True)
    def seed_defc_codes(self):
        clear_defc_rows_cache()
        for code in ["L", "M", "N", "O", "P", "U", "V"]:
            baker.make(
                "references.DisasterEmergencyFundCode",
                code=code,
                public_law=f"PUBLIC LAW FOR CODE {code}",
                title=f"TITLE FOR CODE {code}",
                group_name="covid_19",
            )
        for code in ["Z", "1"]:
            baker.make(
                "references.DisasterEmergencyFundCode",
                code=code,
                public_law=f"PUBLIC LAW FOR CODE {code}",
                title=f"TITLE FOR CODE {code}",
                group_name="infrastructure",
            )
        baker.make(
            "references.DisasterEmergencyFundCode",
            code="A",
            public_law="PUBLIC LAW FOR CODE A",
            title="TITLE FOR CODE A",
            group_name="other",
        )
        yield
        clear_defc_rows_cache()

    def test_def_codes_default_empty(self):
        filters = Filters()

        assert filters.defCodes.require == []
        assert filters.defCodes.exclude == []

    def test_def_codes_accepts_covid_codes(self):
        codes = ["L", "M", "N", "O", "P", "U", "V"]
        filters = Filters(defCodes={"require": codes})

        assert filters.defCodes.require == codes

    def test_def_codes_accepts_infrastructure_codes(self):
        filters = Filters(defCodes={"require": ["Z", "1"]})

        assert filters.defCodes.require == ["Z", "1"]

    def test_def_codes_accepts_require_and_exclude_together(self):
        filters = Filters(defCodes={"require": ["L"], "exclude": ["Z"]})

        assert filters.defCodes.require == ["L"]
        assert filters.defCodes.exclude == ["Z"]

    def test_def_codes_rejects_code_outside_covid_and_infrastructure(self):
        """DEFC codes outside the frontend-supported groups (e.g. a code in another group) are rejected."""
        with pytest.raises(ValidationError) as exc_info:
            Filters(defCodes={"require": ["A"]})

        assert "defCodes" in str(exc_info.value)

    def test_def_codes_rejects_unsupported_code_in_exclude(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(defCodes={"exclude": ["AAA"]})

        assert "defCodes" in str(exc_info.value)


class TestAwardAmounts:
    """Tests for the AwardAmounts range-vs-specific validation on the Filters model"""

    def test_award_amounts_default_empty(self):
        filters = Filters()

        assert filters.awardAmounts.root == {}
        assert filters.model_dump()["awardAmounts"] == {}

    def test_award_amounts_accepts_single_range(self):
        filters = Filters(awardAmounts={"range-1": [1000000, 25000000]})

        assert filters.awardAmounts.root == {"range-1": [1000000, 25000000]}

    def test_award_amounts_accepts_multiple_combinable_ranges(self):
        amounts = {"range-0": [None, 1000000], "range-2": [25000000, 100000000]}
        filters = Filters(awardAmounts=amounts)

        assert filters.awardAmounts.root == amounts

    def test_award_amounts_accepts_specific_alone(self):
        filters = Filters(awardAmounts={"specific": [5000000, 50000000]})

        assert filters.awardAmounts.root == {"specific": [5000000, 50000000]}

    def test_award_amounts_accepts_open_ended_bounds(self):
        """A None bound means the range is open on that end."""
        filters = Filters(awardAmounts={"range-4": [500000000, None]})

        assert filters.awardAmounts.root == {"range-4": [500000000, None]}

    def test_award_amounts_rejects_specific_combined_with_range(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(awardAmounts={"specific": [1, 2], "range-1": [1000000, 25000000]})

        assert "must be the only key" in str(exc_info.value)

    def test_award_amounts_rejects_noncanonical_range_bounds(self):
        """A range bucket must carry its fixed bounds; custom values belong in 'specific'."""
        with pytest.raises(ValidationError) as exc_info:
            Filters(awardAmounts={"range-1": [1000000, 20000000]})

        assert "use the 'specific' key" in str(exc_info.value)

    def test_award_amounts_rejects_unknown_key(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(awardAmounts={"range-9": [0, 1]})

        assert "Invalid award amount key" in str(exc_info.value)

    def test_award_amounts_rejects_wrong_length_pair(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(awardAmounts={"specific": [1000000]})

        assert "must be a [min, max] pair" in str(exc_info.value)

    def test_award_amounts_rejects_max_less_than_min(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(awardAmounts={"specific": [100, 10]})

        assert "greater than or equal to min" in str(exc_info.value)

    def test_award_amounts_rejects_negative_bound(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(awardAmounts={"specific": [-5, 100]})

        assert "non-negative" in str(exc_info.value)

    def test_award_amounts_serialization_preserves_frontend_shape(self):
        """The persisted blob must keep the exact frontend key/pair shape (e.g. 'range-1': [min, max])."""
        filters = Filters(awardAmounts={"range-1": [1000000, 25000000]})

        dumped = filters.model_dump()["awardAmounts"]
        assert dumped == {"range-1": [1000000, 25000000]}

    def test_award_amounts_model_can_be_used_directly(self):
        model = AwardAmounts({"range-3": [100000000, 500000000]})

        assert model.root == {"range-3": [100000000, 500000000]}


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


@pytest.fixture
def psc_codes(db):
    """A small PSC table: R&D (A), Service (B, D) and Product (10) codes across every tier."""
    codes = [
        # Research and Development: AA -> AA1 (tier 3 shares its code with AA10) -> AA11, AA12; AB -> AB20, AB21
        "A",
        "AA",
        "AA10",
        "AA11",
        "AA12",
        "AB20",
        "AB21",
        # Service: B -> B5 -> B502, B503, B504; D -> D3 -> D301
        "B",
        "B5",
        "B502",
        "B503",
        "B504",
        "D",
        "D3",
        "D301",
        # Product
        "10",
        "1005",
        "1010",
    ]
    for code in codes:
        baker.make("references.PSC", code=code, length=len(code), description=f"Description {code}")


@pytest.fixture
def naics_codes(db):
    for code in ["11", "1111", "111110", "111120", "111130", "1112", "111211", "33", "3364", "336411", "336412"]:
        baker.make("references.NAICS", code=code, description=f"Description {code}")


@pytest.fixture
def tas_accounts(db):
    """Two agencies. Agency 072 has federal accounts 072-0300 (2 TAS with awards, 1 without) and 072-1000 (1 TAS)."""
    baker.make(
        "references.ToptierAgency", toptier_agency_id=1, toptier_code="072", name="Agency One", abbreviation="A1"
    )
    baker.make(
        "references.ToptierAgency", toptier_agency_id=2, toptier_code="310", name="Agency Two", abbreviation="A2"
    )
    for fa_id, fa_code, agency_id in [(1, "072-0300", 1), (2, "072-1000", 1), (3, "310-0001", 2)]:
        baker.make(
            "accounts.FederalAccount",
            id=fa_id,
            federal_account_code=fa_code,
            parent_toptier_agency_id=agency_id,
        )

    tas = [
        (1, 1, "072-2020/2021-0300-000", True),
        (2, 1, "072-2021/2022-0300-000", True),
        (3, 1, "072-2022/2023-0300-000", False),  # no award data so it is never counted
        (4, 2, "072-X-1000-000", True),
        (5, 3, "310-X-0001-000", True),
    ]
    for tas_id, fa_id, label, has_award in tas:
        baker.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=tas_id,
            federal_account_id=fa_id,
            tas_rendering_label=label,
        )
        if has_award:
            baker.make("search.AwardSearch", award_id=tas_id)
            baker.make("awards.FinancialAccountsByAwards", treasury_account_id=tas_id, award_id=tas_id)


class TestPscCodeLists:
    """Tests for the Tier1-group-aware require/exclude validation and the calculated counts on PscCodeLists
    (Filters.pscCodes).

    PSC's hierarchy root is one of three synthetic Tier1 group labels (Research and Development,
    Service, Product) rather than a code with its own ancestor chain, so require/exclude are nested
    [Tier1 label, ...PSC code segments] paths instead of flat codes.
    """

    def test_default_empty(self):
        filters = Filters()

        assert filters.pscCodes.require == []
        assert filters.pscCodes.exclude == []
        assert filters.pscCodes.counts == []

    @pytest.mark.parametrize(
        "require, exclude, expected",
        [
            ([["Service"]], [], {"Service": 4}),
            ([["Product"]], [], {"Product": 2}),
            ([["Research and Development"]], [], {"Research and Development": 5}),
            ([["Service", "B"]], [], {"Service": 3}),
            ([["Service", "B", "B5", "B502"]], [], {"Service": 1}),
            # Selecting a middle tier and deselecting one of its leaves
            ([["Service", "B", "B5"]], [["Service", "B", "B5", "B502"]], {"Service": 2}),
            # A second tier code counts everything beneath it
            ([["Research and Development", "AA"]], [], {"Research and Development": 3}),
            # Third tier R&D codes don't own the "0" code, which is a second tier node
            ([["Research and Development", "AA", "AA1"]], [], {"Research and Development": 2}),
            # Several paths in the same group are combined, overlapping paths aren't double counted
            ([["Service", "B"], ["Service", "D"], ["Service", "B", "B5"]], [], {"Service": 4}),
            # Different groups get their own count
            ([["Service", "D"], ["Product"]], [], {"Service": 1, "Product": 2}),
            # An exclusion in one group doesn't touch another group
            ([["Service"], ["Product"]], [["Service", "B"]], {"Service": 1, "Product": 2}),
            # Nothing left over means no entry
            ([["Service", "D"]], [["Service", "D"]], {}),
        ],
    )
    def test_counts_are_calculated_from_require_and_exclude(self, psc_codes, require, exclude, expected):
        filters = Filters(pscCodes={"require": require, "exclude": exclude})

        assert {c.value: c.count for c in filters.pscCodes.counts} == expected
        assert all(c.label == "" for c in filters.pscCodes.counts)

    def test_counts_provided_by_the_llm_are_ignored(self, psc_codes):
        filters = Filters(
            pscCodes={"require": [["Service", "D"]], "counts": [{"value": "Service", "label": "", "count": 999}]}
        )

        assert [(c.value, c.count) for c in filters.pscCodes.counts] == [("Service", 1)]

    def test_counts_are_included_when_dumped(self, psc_codes):
        filters = Filters(pscCodes={"require": [["Service", "D"]]})

        assert filters.model_dump()["pscCodes"]["counts"] == [{"value": "Service", "label": "", "count": 1}]

    def test_require_rejects_root_not_a_tier1_label(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(pscCodes={"require": [["AC"]]})

        assert "must be one of" in str(exc_info.value)

    def test_require_rejects_empty_path(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(pscCodes={"require": [[]]})

        assert "must be one of" in str(exc_info.value)

    def test_require_rejects_malformed_code_segment(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(pscCodes={"require": [["Service", "too-long-code"]]})

        assert "one to four character" in str(exc_info.value)

    def test_exclude_uses_same_path_validation_as_require(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(pscCodes={"exclude": [["not-a-group"]]})

        assert "must be one of" in str(exc_info.value)

    def test_psc_code_lists_model_can_be_used_directly(self, psc_codes):
        model = PscCodeLists(require=[["Service", "D"]])

        assert model.require == [["Service", "D"]]
        assert [c.model_dump() for c in model.counts] == [{"value": "Service", "label": "", "count": 1}]


class TestTasCodeLists:
    """Tests for the hierarchical AID / AID-MAIN / full-label path validation and the calculated counts on
    TasCodeLists (Filters.tasCodes)."""

    def test_default_empty(self):
        filters = Filters()

        assert filters.tasCodes.require == []
        assert filters.tasCodes.exclude == []
        assert filters.tasCodes.counts == []

    @pytest.mark.parametrize(
        "require, exclude, expected",
        [
            # Only TAS with award data are counted
            ([["072"]], [], {"072": 3}),
            ([["072", "072-0300"]], [], {"072": 2}),
            ([["072", "072-0300", "072-2020/2021-0300-000"]], [], {"072": 1}),
            # Selecting an agency and deselecting one of its federal accounts
            ([["072"]], [["072", "072-0300"]], {"072": 1}),
            ([["072"]], [["072", "072-0300", "072-2020/2021-0300-000"]], {"072": 2}),
            ([["072"], ["310"]], [["072", "072-1000"]], {"072": 2, "310": 1}),
            ([["072", "072-1000"]], [["072"]], {}),
        ],
    )
    def test_counts_are_calculated_from_require_and_exclude(self, tas_accounts, require, exclude, expected):
        filters = Filters(tasCodes={"require": require, "exclude": exclude})

        assert {c.value: c.count for c in filters.tasCodes.counts} == expected

    def test_count_label_is_the_agency_name_and_abbreviation(self, tas_accounts):
        filters = Filters(tasCodes={"require": [["310"]]})

        assert [c.model_dump() for c in filters.tasCodes.counts] == [
            {"value": "310", "label": "Agency Two (A2)", "count": 1}
        ]

    def test_counts_provided_by_the_llm_are_ignored(self, tas_accounts):
        filters = Filters(tasCodes={"require": [["310"]], "counts": [{"value": "310", "label": "Wrong", "count": 999}]})

        assert [(c.label, c.count) for c in filters.tasCodes.counts] == [("Agency Two (A2)", 1)]

    def test_require_rejects_path_over_three_elements(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(tasCodes={"require": [["535", "535-2724", "535-2020/2021-2724-000", "extra"]]})

        assert "between 1 and 3 elements" in str(exc_info.value)

    def test_require_rejects_empty_path(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(tasCodes={"require": [[]]})

        assert "between 1 and 3 elements" in str(exc_info.value)

    def test_exclude_uses_same_path_validation_as_require(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(tasCodes={"exclude": [["a", "b", "c", "d"]]})

        assert "between 1 and 3 elements" in str(exc_info.value)

    def test_tas_code_lists_model_can_be_used_directly(self, tas_accounts):
        model = TasCodeLists(require=[["310"]])

        assert model.require == [["310"]]
        assert [c.count for c in model.counts] == [1]


class TestNaicsCodeLists:
    """Tests for the calculated counts on NaicsCodeLists (Filters.naicsCodes)."""

    def test_default_empty(self):
        assert Filters().naicsCodes.counts == []

    @pytest.mark.parametrize(
        "require, exclude, expected",
        [
            # Top tier covers every 6-digit code beneath it
            (["11"], [], {"11": 4}),
            # Middle tier
            (["1111"], [], {"11": 3}),
            # Middle tier with one leaf deselected
            (["1111"], ["111110"], {"11": 2}),
            # Explicit leaves, as the frontend sends them after a deselection
            (["111120", "111130"], ["111110"], {"11": 2}),
            (["111110"], [], {"11": 1}),
            # Roots are kept separate and combined across codes that share one
            (["1111", "1112", "3364"], [], {"11": 4, "33": 2}),
            (["11", "33"], ["3364"], {"11": 4}),
            (["1111"], ["11"], {}),
        ],
    )
    def test_counts_are_calculated_from_require_and_exclude(self, naics_codes, require, exclude, expected):
        filters = Filters(naicsCodes={"require": require, "exclude": exclude})

        assert {c.value: c.count for c in filters.naicsCodes.counts} == expected

    def test_count_label_is_the_root_description(self, naics_codes):
        filters = Filters(naicsCodes={"require": ["336411"]})

        assert [c.model_dump() for c in filters.naicsCodes.counts] == [
            {"value": "33", "label": "Description 33", "count": 1}
        ]

    def test_counts_provided_by_the_llm_are_ignored(self, naics_codes):
        model = NaicsCodeLists(
            require=["336411", "336412"], counts=[{"value": "33", "label": "Manufacturing", "count": 99}]
        )

        assert model.require == ["336411", "336412"]
        assert [(c.label, c.count) for c in model.counts] == [("Description 33", 2)]

    def test_rejects_nested_path_shape(self):
        """A PSC/TAS-style nested path must not silently work its way into NAICS's flat require list."""
        with pytest.raises(ValidationError) as exc_info:
            NaicsCodeLists(require=[["336411"]])

        assert "require" in str(exc_info.value)


class TestCodeListsSchema:
    """'counts' is calculated, so the llm-facing (validation) schema must not ask the llm to populate it."""

    @pytest.mark.parametrize("field", ["naicsCodes", "pscCodes", "tasCodes"])
    def test_counts_not_in_llm_schema(self, field):
        schema = ExecuteFilterInput.model_json_schema()
        definition = schema["$defs"][schema["properties"][field]["$ref"].split("/")[-1]]

        assert set(definition["properties"]) == {"require", "exclude"}


class TestSelectedCFDA:
    """Tests for the SelectedCFDA model and its wiring into Filters/ExecuteFilterInput.selectedCFDA
    (keyed by CFDA program number, mirroring the SelectedAgency re-keying pattern)."""

    def test_accepts_valid_entry(self):
        cfda = SelectedCFDA(identifier="10.557", program_number="10.557", program_title="WIC")

        assert cfda.program_title == "WIC"
        assert cfda.popular_name is None

    def test_accepts_optional_popular_name(self):
        cfda = SelectedCFDA(identifier="10.557", program_number="10.557", program_title="WIC", popular_name="WIC")

        assert cfda.popular_name == "WIC"

    def test_identifier_must_match_program_number(self):
        with pytest.raises(ValidationError) as exc_info:
            SelectedCFDA(identifier="10.557", program_number="10.999", program_title="WIC")

        assert "identifier' must equal 'program_number'" in str(exc_info.value)

    def test_dict_key_must_equal_identifier(self):
        with pytest.raises(ValidationError) as exc_info:
            Filters(
                selectedCFDA={"10.999": {"identifier": "10.557", "program_number": "10.557", "program_title": "WIC"}}
            )

        assert "must equal its entry's 'identifier'" in str(exc_info.value)

    def test_dict_key_matching_identifier_is_accepted(self):
        filters = Filters(
            selectedCFDA={"10.557": {"identifier": "10.557", "program_number": "10.557", "program_title": "WIC"}}
        )

        assert set(filters.selectedCFDA.keys()) == {"10.557"}

    def test_multiple_entries_each_correctly_keyed(self):
        filters = Filters(
            selectedCFDA={
                "10.557": {"identifier": "10.557", "program_number": "10.557", "program_title": "WIC"},
                "93.558": {"identifier": "93.558", "program_number": "93.558", "program_title": "TANF"},
            }
        )

        assert set(filters.selectedCFDA.keys()) == {"10.557", "93.558"}

    def test_empty_dict_is_valid(self):
        filters = Filters(selectedCFDA={})

        assert filters.selectedCFDA == {}

    def test_execute_filter_input_also_validates_key_identifier_match(self):
        """ExecuteFilterInput has its own copy of the selectedCFDA validator; both models must agree."""
        with pytest.raises(ValidationError) as exc_info:
            ExecuteFilterInput(
                selectedCFDA={"10.999": {"identifier": "10.557", "program_number": "10.557", "program_title": "WIC"}}
            )

        assert "must equal its entry's 'identifier'" in str(exc_info.value)
