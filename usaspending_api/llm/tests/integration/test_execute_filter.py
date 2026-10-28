import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from asgiref.sync import sync_to_async
from model_bakery import baker

from usaspending_api.llm.models.py_models import ExecuteFilterInput, Filters, aget_defc_rows, clear_defc_rows_cache
from usaspending_api.llm.tools.exceptions import (
    AgencyNotRecognizedError,
    AwardAmountNotParseableError,
    AwardNotRecognizedError,
    DefcCodeNotRecognizedError,
    FilterValidationError,
    RecipientNotRecognizedError,
    TimePeriodNotParseableError,
)
from usaspending_api.llm.tools.execute_filter import execute_filter, execute_filter_tool
from usaspending_api.references.models import FilterHash

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture(autouse=True)
def mock_existence_lookups():
    """Default lookup-tool mocks so pre-existing tests (not specifically exercising the new
    existence-validation paths) continue to pass without hitting real DB/OpenSearch backends.
    Tests exercising validation failures override these within their own nested `patch(...)`."""

    async def agency_lookup(query, **kwargs):
        return {"results": {"1_toptier": {"id": 1, "agencyType": "toptier"}}, "instructions": None}

    async def recipient_lookup(query, **kwargs):
        return {"recipient_names": [query]}

    async def award_lookup(query, **kwargs):
        return {"result": {"generated_unique_award_id": query}}

    with (
        patch("usaspending_api.llm.tools.execute_filter.lookup_agency_tool.function", side_effect=agency_lookup),
        patch("usaspending_api.llm.tools.execute_filter.lookup_recipient_tool.function", side_effect=recipient_lookup),
        patch("usaspending_api.llm.tools.execute_filter.lookup_award_tool.function", side_effect=award_lookup),
    ):
        yield


@pytest.fixture
def sample_filters():
    """Fixture providing sample valid filter data."""
    return {
        "time_period": [{"start_date": "2023-01-01", "end_date": "2023-12-31"}],
        "timePeriodType": "dr",
        "awardType": ["A", "B"],
        "selectedAwardingAgencies": {
            "1_toptier": {
                "id": 1,
                "agencyType": "toptier",
                "toptier_flag": True,
                "toptier_agency": {
                    "id": 1,
                    "toptier_code": "001",
                    "abbreviation": "USDA",
                    "name": "Department of Agriculture",
                },
            }
        },
    }


@pytest.fixture
def mock_filter_hash():
    """Fixture to mock FilterHash model."""
    with patch("usaspending_api.llm.tools.execute_filter.FilterHash") as mock:
        mock.DoesNotExist = FilterHash.DoesNotExist
        mock.objects.aget = AsyncMock()
        yield mock


def _new_mock_instance():
    mock_instance = MagicMock()
    mock_instance.asave = AsyncMock()
    return mock_instance


class TestInputValidation:
    """Test input validation and error handling."""

    async def test_valid_filters_accepted(self, sample_filters):
        """Test that valid filters are accepted."""
        result = await execute_filter(**sample_filters)

        assert "hash" in result
        assert "error" not in result

    async def test_invalid_filters_return_error(self):
        """Test that invalid filters return validation error."""
        result = await execute_filter(invalid_field="invalid_value")

        assert "error" in result
        assert "message" in result
        assert "invalid" in result["message"].lower()

    async def test_empty_filters_accepted(self):
        """Test that empty filters are valid."""
        result = await execute_filter()

        assert "hash" in result
        assert "error" not in result

    async def test_validation_error_message_includes_details(self):
        """Test that validation errors include helpful details."""
        result = await execute_filter(awardType="invalid")

        assert "error" in result
        assert isinstance(result["error"], str)
        assert len(result["error"]) > 0

    async def test_partial_valid_filters(self):
        """Test filters with only some valid fields."""
        result = await execute_filter(awardType=["A", "B"], invalid_field="should_be_ignored")

        # Should fail validation due to invalid field
        assert "error" in result

    async def test_selected_recipients_at_max_length_accepted(self):
        """Test that exactly 50 selected recipients is accepted."""
        result = await execute_filter(selectedRecipients=[f"RECIPIENT_{i}" for i in range(50)])

        assert "hash" in result
        assert "error" not in result

    async def test_selected_recipients_over_max_length_returns_error(self):
        """Test that more than 50 selected recipients is rejected to avoid oversized OpenSearch queries."""
        result = await execute_filter(selectedRecipients=[f"RECIPIENT_{i}" for i in range(51)])

        assert "error" in result
        assert "message" in result


class TestFilterProcessing:
    """Test filter processing and transformation."""

    async def test_filters_converted_to_filter_request(self, sample_filters, mock_filter_hash):
        """Test that filters are properly converted to FilterRequest format."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_instance.hash = "test_hash_value"
        mock_filter_hash.return_value = mock_instance

        result = await execute_filter(**sample_filters)

        assert "hash" in result
        assert mock_filter_hash.called
        assert mock_instance.asave.called
        call_args = mock_filter_hash.call_args[1]
        saved_filter = call_args["filter"]
        assert "filters" in saved_filter

    async def test_exclude_none_values(self, mock_filter_hash):
        """Test that None values are excluded from filter request."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        await execute_filter(awardType=["A"])

        call_args = mock_filter_hash.call_args
        saved_filter = call_args[1]["filter"]

        # Should only contain non-None fields
        assert "awardType" in saved_filter["filters"]
        # None fields should not be present
        assert all(v is not None for v in saved_filter["filters"].values())

    async def test_keyword_transformation(self, mock_filter_hash):
        """Test that keyword field is transformed to dict format."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        await execute_filter(keyword=["test", "search"])

        call_args = mock_filter_hash.call_args
        saved_filter = call_args[1]["filter"]

        # Keyword should be transformed to {value: value} format
        assert "keyword" in saved_filter["filters"]
        keyword_dict = saved_filter["filters"]["keyword"]
        assert isinstance(keyword_dict, dict)
        assert keyword_dict == {"test": "test", "search": "search"}

    async def test_keyword_empty_list(self, mock_filter_hash):
        """Test handling of empty keyword list."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        await execute_filter(keyword=[])

        call_args = mock_filter_hash.call_args
        saved_filter = call_args[1]["filter"]

        # Empty keyword should result in empty dict
        if "keyword" in saved_filter["filters"]:
            assert saved_filter["filters"]["keyword"] == {}

    async def test_filter_json_sorted_keys(self, mock_filter_hash):
        """Test that filter JSON has sorted keys for consistent hashing."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        # Create filters with multiple fields
        result = await execute_filter(
            awardType=["A"],
            selectedAwardingAgencies={
                "1_toptier": {
                    "id": 1,
                    "agencyType": "toptier",
                    "toptier_flag": True,
                    "toptier_agency": {
                        "id": 1,
                        "toptier_code": "001",
                        "abbreviation": "USDA",
                        "name": "Department of Agriculture",
                    },
                }
            },
            timePeriodType="dr",
            time_period=[{"start_date": "2023-01-01", "end_date": "2023-12-31"}],
        )

        # Verify hash was created (keys were sorted)
        assert "hash" in result


class TestHashCreationAndStorage:
    """Test hash creation and database storage."""

    @patch("usaspending_api.llm.tools.execute_filter.create_hash")
    async def test_hash_created_from_filter_json(self, mock_create_hash, mock_filter_hash):
        """Test that hash is created from filter JSON."""
        mock_create_hash.return_value = "test_hash_123"
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        result = await execute_filter(awardType=["A"])

        # Verify create_hash was called
        assert mock_create_hash.called
        # Verify hash is returned
        assert result["hash"] == "test_hash_123"

    async def test_existing_hash_not_recreated(self, mock_filter_hash):
        """Test that existing hash is returned without creating new entry."""
        existing_hash = MagicMock()
        existing_hash.hash = "existing_hash_456"
        mock_filter_hash.objects.aget.return_value = existing_hash

        result = await execute_filter(awardType=["A"])

        # Should return existing hash
        assert "hash" in result
        # Should not create new FilterHash
        assert not mock_filter_hash.called

    async def test_new_hash_saved_to_database(self, mock_filter_hash):
        """Test that new hash is saved to database."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        result = await execute_filter(awardType=["A"])

        # Verify FilterHash was created
        assert mock_filter_hash.called
        # Verify save was called
        assert mock_instance.asave.called
        # Verify hash is returned
        assert "hash" in result

    async def test_hash_includes_filter_json(self, mock_filter_hash):
        """Test that saved hash includes filter JSON."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        await execute_filter(awardType=["A", "B"])

        # Verify FilterHash was called with filter JSON
        call_args = mock_filter_hash.call_args
        assert "filter" in call_args[1]
        saved_filter = call_args[1]["filter"]
        assert "filters" in saved_filter
        assert saved_filter["filters"]["awardType"] == ["A", "B"]

    async def test_same_filters_produce_same_hash(self, mock_filter_hash):
        """Test that identical filters produce identical hashes."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        filters = {"awardType": ["A", "B"]}

        result1 = await execute_filter(**filters)
        result2 = await execute_filter(**filters)

        # Both should produce the same hash
        assert result1["hash"] == result2["hash"]

    async def test_different_filters_produce_different_hashes(self, mock_filter_hash):
        """Test that different filters produce different hashes."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        result1 = await execute_filter(awardType=["A"])
        result2 = await execute_filter(awardType=["B"])

        # Should produce different hashes
        assert result1["hash"] != result2["hash"]


class TestErrorHandling:
    """Test error handling scenarios."""

    async def test_validation_error_returns_error_dict(self):
        """Test that validation errors return proper error dict."""
        result = await execute_filter(awardType="invalid_format")

        assert "error" in result
        assert "message" in result
        assert isinstance(result["error"], str)
        assert isinstance(result["message"], str)

    async def test_database_save_error_returns_error_dict(self, mock_filter_hash):
        """Test that database save errors return proper error dict."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_instance.asave.side_effect = Exception("Database error")
        mock_filter_hash.return_value = mock_instance

        result = await execute_filter(awardType=["A"])

        assert "error" in result
        assert "message" in result
        assert "error saving" in result["message"].lower()

    async def test_error_message_includes_exception_details(self, mock_filter_hash):
        """Test that error messages include exception details."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_instance.asave.side_effect = Exception("Specific database error")
        mock_filter_hash.return_value = mock_instance

        result = await execute_filter(awardType=["A"])

        assert "Specific database error" in result["error"]

    async def test_validation_error_does_not_save_to_database(self, mock_filter_hash):
        """Test that validation errors don't attempt database save."""
        await execute_filter(invalid_field="invalid")

        # Should not attempt to access database
        assert not mock_filter_hash.objects.aget.called
        assert not mock_filter_hash.called


class TestValidationErrorTranslation:
    """Single-category pydantic validation failures (DEFC, award amount, time period) are raised
    as specific exceptions instead of being swallowed into a generic error dict, so the assistant
    can show a clean user-facing message while still receiving the full pydantic detail for
    self-correction. Failures spanning more than one category are raised as a generic
    FilterValidationError for the same reason -- a returned error dict never reaches the frontend
    as a visible error, even though it looks like one to the model. An unrecognized field by
    itself keeps the original generic-error-dict behavior."""

    @pytest.fixture(autouse=True)
    async def seed_defc_codes(self):
        clear_defc_rows_cache()
        await sync_to_async(baker.make)(
            "references.DisasterEmergencyFundCode",
            code="L",
            public_law="PUBLIC LAW FOR CODE L",
            title="TITLE FOR CODE L",
            group_name="covid_19",
        )
        # Pre-warm the module-level cache via the async-safe entry point -- the pydantic
        # validator in DEFCodeLists only has a synchronous entry point, which would raise
        # SynchronousOnlyOperation if it had to hit the DB for the first time from here.
        await aget_defc_rows()
        yield
        clear_defc_rows_cache()

    async def test_defc_only_failure_raises_specific_error(self):
        with pytest.raises(DefcCodeNotRecognizedError) as exc_info:
            await execute_filter(defCodes={"require": ["NOT_REAL"]})

        assert exc_info.value.user_message == "DEFC code not found: NOT_REAL."
        assert "Invalid DEFC code(s)" in exc_info.value.model_detail

    async def test_award_amount_business_rule_failure_raises_specific_error(self):
        with pytest.raises(AwardAmountNotParseableError) as exc_info:
            await execute_filter(awardAmounts={"specific": [1, 2], "range-1": [1000000, 25000000]})

        assert "Unable to filter awards based on the criteria" in exc_info.value.user_message
        assert "must be the only key" in exc_info.value.model_detail

    async def test_award_amount_type_coercion_failure_raises_specific_error(self):
        """A non-numeric bound fails pydantic's own int coercion, bypassing the custom validator --
        it must still map to the same user-facing error."""
        with pytest.raises(AwardAmountNotParseableError):
            await execute_filter(awardAmounts={"specific": ["not-a-number", None]})

    async def test_time_period_only_failure_raises_specific_error(self):
        with pytest.raises(TimePeriodNotParseableError) as exc_info:
            await execute_filter(
                timePeriodType="dr", time_period=[{"start_date": "invalid-date", "end_date": "2023-12-31"}]
            )

        assert exc_info.value.user_message == (
            "Unable to filter based on the specified time period. Try rephrasing the dates you're looking for."
        )
        assert "not a valid date" in exc_info.value.model_detail

    async def test_mixed_category_failure_raises_generic_validation_error(self):
        """Errors spanning more than one recognized category are raised, not returned as a dict,
        so the assistant emits a visible tool_error event to the frontend instead of the tool call
        looking like a success."""
        with pytest.raises(FilterValidationError) as exc_info:
            await execute_filter(
                defCodes={"require": ["NOT_REAL"]},
                timePeriodType="dr",
                time_period=[{"start_date": "invalid-date", "end_date": "2023-12-31"}],
            )

        assert "invalid" in exc_info.value.user_message.lower()
        assert "NOT_REAL" in exc_info.value.model_detail


class TestAwardTypeNames:
    """The LLM selects award types by frontend name; execute_filter persists the resolved codes."""

    async def test_award_type_names_are_persisted_as_codes(self, mock_filter_hash):
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_filter_hash.return_value = _new_mock_instance()

        result = await execute_filter(awardType=["Grants", "Direct Loans"])

        assert "hash" in result
        saved_filter = mock_filter_hash.call_args.kwargs["filter"]
        assert saved_filter["filters"]["awardType"] == ["02", "03", "04", "07", "F001", "F003"]

    async def test_legacy_and_current_codes_are_still_accepted(self, mock_filter_hash):
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_filter_hash.return_value = _new_mock_instance()

        result = await execute_filter(awardType=["02", "F002"])

        assert "hash" in result
        saved_filter = mock_filter_hash.call_args.kwargs["filter"]
        assert saved_filter["filters"]["awardType"] == ["02", "03", "04", "05", "F001", "F002"]

    async def test_unknown_award_type_name_returns_guidance(self):
        result = await execute_filter(awardType=["Fellowships"])

        assert "error" in result
        assert "Unknown awardType value(s): ['Fellowships']" in result["error"]
        assert "omit awardType" in result["error"]


class TestAgencyExistenceValidation:
    """selectedAwardingAgencies/selectedFundingAgencies entries are confirmed via lookup_agency_tool
    before a filter is hashed."""

    async def test_valid_agency_passes(self, sample_filters, mock_filter_hash):
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        result = await execute_filter(**sample_filters)

        assert "hash" in result

    async def test_unresolvable_agency_raises(self, sample_filters):
        with patch(
            "usaspending_api.llm.tools.execute_filter.lookup_agency_tool.function",
            new_callable=AsyncMock,
            return_value={"results": {}, "instructions": None},
        ):
            with pytest.raises(AgencyNotRecognizedError) as exc_info:
                await execute_filter(**sample_filters)

        assert "Department of Agriculture" in exc_info.value.user_message
        assert "Unresolvable selected agencies" in exc_info.value.model_detail

    async def test_agency_lookup_zero_results_is_treated_as_unresolvable(self, sample_filters):
        """lookup_agency_tool raises AgencyNotRecognizedError on zero matches; that must be caught
        and translated into the same unresolvable-agency error, not propagated raw."""
        with patch(
            "usaspending_api.llm.tools.execute_filter.lookup_agency_tool.function",
            new_callable=AsyncMock,
            side_effect=AgencyNotRecognizedError("Unable to filter. We couldn't find an agency with the name: 001"),
        ):
            with pytest.raises(AgencyNotRecognizedError) as exc_info:
                await execute_filter(**sample_filters)

        assert "Unresolvable selected agencies" in exc_info.value.model_detail


class TestRecipientExistenceValidation:
    """selectedRecipients entries are confirmed via lookup_recipient_tool before a filter is hashed."""

    async def test_valid_recipient_passes(self, mock_filter_hash):
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        with patch(
            "usaspending_api.llm.tools.execute_filter.lookup_recipient_tool.function",
            new_callable=AsyncMock,
            return_value={"recipient_names": ["ACME CORPORATION"]},
        ):
            result = await execute_filter(selectedRecipients=["ACME Corporation"])

        assert "hash" in result

    async def test_unresolvable_recipient_raises(self):
        with patch(
            "usaspending_api.llm.tools.execute_filter.lookup_recipient_tool.function",
            new_callable=AsyncMock,
            return_value={"recipient_names": []},
        ):
            with pytest.raises(RecipientNotRecognizedError) as exc_info:
                await execute_filter(selectedRecipients=["Totally Fake Recipient"])

        assert "Totally Fake Recipient" in exc_info.value.user_message
        assert "Unresolvable selected recipients" in exc_info.value.model_detail

    async def test_multiple_unresolvable_recipients_are_all_listed(self):
        with patch(
            "usaspending_api.llm.tools.execute_filter.lookup_recipient_tool.function",
            new_callable=AsyncMock,
            return_value={"recipient_names": []},
        ):
            with pytest.raises(RecipientNotRecognizedError) as exc_info:
                await execute_filter(selectedRecipients=["Fake One", "Fake Two"])

        assert "Fake One" in exc_info.value.user_message
        assert "Fake Two" in exc_info.value.user_message


class TestAwardIdExistenceValidation:
    """selectedAwardIDs entries are confirmed via the new lookup_award_tool before a filter is hashed."""

    async def test_valid_award_id_passes(self, mock_filter_hash):
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        with patch(
            "usaspending_api.llm.tools.execute_filter.lookup_award_tool.function",
            new_callable=AsyncMock,
            return_value={"result": {"generated_unique_award_id": "CONT_AWD_123"}},
        ):
            result = await execute_filter(selectedAwardIDs=["CONT_AWD_123"])

        assert "hash" in result

    async def test_generated_award_id_is_replaced_with_display_award_id(self, mock_filter_hash):
        """The award ID search filter only matches PIID/FAIN/URI, so a generated_unique_award_id the model
        passes must be persisted as the award's display ID."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_filter_hash.return_value = _new_mock_instance()

        with patch(
            "usaspending_api.llm.tools.execute_filter.lookup_award_tool.function",
            new_callable=AsyncMock,
            return_value={"award_id": "W56HZV07C0096", "result": {}},
        ):
            await execute_filter(selectedAwardIDs=["CONT_AWD_W56HZV07C0096_9700_-NONE-_-NONE-", "W56HZV07C0096"])

        saved_filter = mock_filter_hash.call_args.kwargs["filter"]
        assert saved_filter["filters"]["selectedAwardIDs"] == {"W56HZV07C0096": "W56HZV07C0096"}

    async def test_unresolvable_award_id_raises(self):
        with patch(
            "usaspending_api.llm.tools.execute_filter.lookup_award_tool.function",
            new_callable=AsyncMock,
            side_effect=AwardNotRecognizedError("Unable to filter. We couldn't find an award with the ID: x"),
        ):
            with pytest.raises(AwardNotRecognizedError) as exc_info:
                await execute_filter(selectedAwardIDs=["FAKE_AWARD_ID"])

        assert "FAKE_AWARD_ID" in exc_info.value.user_message
        assert "Unresolvable selected award IDs" in exc_info.value.model_detail
        assert "lookup_recipient" in exc_info.value.model_detail
        assert "lookup_recipient" not in exc_info.value.user_message


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

    def test_tool_input_schema_is_decoupled_from_filter_model(self):
        """The ExecuteFilterInput schema is decoupled from the full Filters model."""
        schema = execute_filter_tool.description.input_schema

        assert schema == ExecuteFilterInput.model_json_schema()
        assert schema != Filters.model_json_schema()

    def test_execute_filter_input_exposes_every_filter_field(self):
        """The assistant must be able to see and set every Filters field, except documented legacy fields.

        Field-set equality is the drift guard in both directions: a Filters-only field would be a
        filter the assistant can't set, and an ExecuteFilterInput-only field would be rejected by
        Filters(**kwargs) at runtime (extra='forbid'). `defCode` is a deliberate, documented
        exception: it's a legacy singular field kept on `Filters` so old persisted FilterHash blobs
        still validate, but it's superseded by `defCodes` (require/exclude) and intentionally kept
        off the tool surface so the assistant never emits it.
        """
        legacy_filters_only_fields = {"defCode"}
        assert set(Filters.model_fields) - set(ExecuteFilterInput.model_fields) == legacy_filters_only_fields
        assert set(ExecuteFilterInput.model_fields) - set(Filters.model_fields) == set()

    def test_execute_filter_input_omits_the_large_enums(self):
        """ExecuteFilterInput carries loose str types for recipientType/DEFC instead of the big enums.

        This reduces the payload; the 68-value recipient-type enum is looked up via tools rather than
        inlined into the schema sent on every converse call.
        """
        efi = ExecuteFilterInput.model_json_schema()
        efi_json = json.dumps(efi)

        # recipientType is a plain list[str] here; the enum values are not inlined.
        assert efi["properties"]["recipientType"]["items"] == {"type": "string"}
        assert "corporate_entity_tax_exempt" not in efi_json

        # The validation model, by contrast, does pin the enum.
        assert "corporate_entity_tax_exempt" in json.dumps(Filters.model_json_schema())

    def test_logging_function_handles_empty_input(self):
        """Test that logging function handles empty input."""
        log_msg = execute_filter_tool.logging({})

        assert isinstance(log_msg, str)
        assert "Selecting filters" in log_msg

    async def test_tool_execution_through_aitool(self, mock_filter_hash):
        """Test executing tool through AITool interface."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        result = await execute_filter_tool.function(awardType=["A"])

        assert "hash" in result


class TestIntegrationWithFiltersModel:
    """Test integration with Filters Pydantic model."""

    async def test_all_filters_model_fields_supported(self):
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
                test_data["timePeriodType"] = "dr"
                test_data[field_name] = [{"start_date": "2023-01-01", "end_date": "2023-12-31"}]
            elif field_name == "keyword":
                test_data[field_name] = ["test"]

        # Should not raise validation error
        result = await execute_filter(**test_data)
        assert "hash" in result or "error" in result

    async def test_filters_model_validation_applied(self):
        """An unparseable date raises the specific time-period error instead of validating."""
        with pytest.raises(TimePeriodNotParseableError):
            await execute_filter(
                timePeriodType="dr", time_period=[{"start_date": "invalid-date", "end_date": "2023-12-31"}]
            )

    async def test_complex_filters_combination(self, mock_filter_hash):
        """Test complex combination of multiple filters."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        result = await execute_filter(
            awardType=["A", "B", "C"],
            timePeriodType="dr",
            time_period=[{"start_date": "2023-01-01", "end_date": "2023-12-31"}],
            selectedAwardingAgencies={
                "1_toptier": {
                    "id": 1,
                    "agencyType": "toptier",
                    "toptier_flag": True,
                    "toptier_agency": {
                        "id": 1,
                        "toptier_code": "001",
                        "abbreviation": "USDA",
                        "name": "Department of Agriculture",
                    },
                }
            },
            keyword=["fire", "prevention"],
        )

        assert "hash" in result


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    async def test_very_large_filter_set(self, mock_filter_hash):
        """Test handling of very large filter sets."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        # Create large filter set (keyword accepts arbitrary strings; awardType is validated
        # against a known code set, so use keyword to exercise raw payload size here).
        large_keywords = [f"term_{i}" for i in range(100)]

        result = await execute_filter(keyword=large_keywords)

        assert "hash" in result

    async def test_unicode_in_filters(self, mock_filter_hash):
        """Test handling of unicode characters in filters."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        result = await execute_filter(keyword=["test™", "café", "日本"])

        assert "hash" in result

    async def test_special_characters_in_keyword(self, mock_filter_hash):
        """Test handling of special characters in keyword field."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        result = await execute_filter(keyword=["test & co.", "50% off", "$1,000"])

        assert "hash" in result

    async def test_nested_filter_structures(self, mock_filter_hash):
        """Test handling of deeply nested filter structures."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        result = await execute_filter(
            selectedAwardingAgencies={
                "1_toptier": {
                    "id": 1,
                    "agencyType": "toptier",
                    "toptier_flag": True,
                    "toptier_agency": {
                        "id": 1,
                        "toptier_code": "001",
                        "abbreviation": "USDA",
                        "name": "Department of Agriculture",
                    },
                }
            },
        )

        assert "hash" in result

    async def test_empty_list_filters(self, mock_filter_hash):
        """Test handling of empty list filters."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        result = await execute_filter(awardType=[], keyword=[])

        assert "hash" in result


class TestRealWorldScenarios:
    """Test realistic usage scenarios."""

    async def test_typical_award_search_filters(self, mock_filter_hash):
        """Test typical award search filter combination."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        result = await execute_filter(
            timePeriodType="dr",
            time_period=[{"start_date": "2023-01-01", "end_date": "2023-12-31"}],
            awardType=["A", "B", "C", "D"],
            selectedAwardingAgencies={
                "1_toptier": {
                    "id": 1,
                    "agencyType": "toptier",
                    "toptier_flag": True,
                    "toptier_agency": {
                        "id": 1,
                        "toptier_code": "001",
                        "abbreviation": "USDA",
                        "name": "Department of Agriculture",
                    },
                }
            },
            keyword=["forestry", "prevention"],
        )

        assert "hash" in result

    async def test_recipient_focused_search(self, mock_filter_hash):
        """Test recipient-focused search filters."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        result = await execute_filter(selectedRecipients=["ACME Corporation"], recipientType=["business"])

        assert "hash" in result

    async def test_location_based_search(self, mock_filter_hash):
        """Test location-based search filters."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        result = await execute_filter(
            selectedLocations={
                "USA_TX": {
                    "identifier": "USA_TX",
                    "filter": {"country": "USA", "state": "TX"},
                    "display": {"entity": "State", "standalone": "Texas", "title": "TEXAS"},
                }
            }
        )

        assert "hash" in result

    async def test_award_amounts_range_filter(self, mock_filter_hash):
        """A valid award-amount range blob is accepted and hashed."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        result = await execute_filter(awardAmounts={"range-1": [1000000, 25000000]})

        assert "hash" in result
        saved_filter = mock_filter_hash.call_args[1]["filter"]
        assert saved_filter["filters"]["awardAmounts"] == {"range-1": [1000000, 25000000]}

    async def test_award_amounts_specific_filter(self, mock_filter_hash):
        """A valid 'specific' award-amount blob is accepted."""
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        result = await execute_filter(awardAmounts={"specific": [5000000, None]})

        assert "hash" in result

    async def test_award_amounts_invalid_returns_error(self):
        """Combining 'specific' with a range bucket raises the specific award-amount error before any DB access."""
        with pytest.raises(AwardAmountNotParseableError):
            await execute_filter(awardAmounts={"specific": [1, 2], "range-1": [1000000, 25000000]})

    async def test_filter_reuse_returns_same_hash(self, mock_filter_hash):
        """Test that reusing same filters returns same hash."""
        # First call creates hash
        mock_filter_hash.objects.aget.side_effect = FilterHash.DoesNotExist
        mock_instance = _new_mock_instance()
        mock_filter_hash.return_value = mock_instance

        filters = {"awardType": ["A", "B"]}
        result1 = await execute_filter(**filters)

        # Second call finds existing hash
        mock_filter_hash.objects.aget.side_effect = None
        existing = MagicMock()
        existing.hash = result1["hash"]
        mock_filter_hash.objects.aget.return_value = existing

        result2 = await execute_filter(**filters)

        assert result1["hash"] == result2["hash"]
