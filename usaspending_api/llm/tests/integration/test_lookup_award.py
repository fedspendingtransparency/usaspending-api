import itertools
import uuid

import pytest
from asgiref.sync import sync_to_async
from model_bakery import baker

from usaspending_api.llm.tools.exceptions import AwardNotRecognizedError
from usaspending_api.llm.tools.lookup_award import AwardLookupTool, lookup_award_tool
from usaspending_api.search.models.award_search import AwardSearch

pytestmark = pytest.mark.django_db(transaction=True)

_award_id_counter = itertools.count(1)


async def _make_award_search(**kwargs):
    defaults = {
        "award_id": next(_award_id_counter),
        "certified_date": "2020-01-01",
        "generated_unique_award_id": f"TEST_AWARD_{uuid.uuid4()}",
    }
    defaults.update(kwargs)
    return await sync_to_async(baker.make)(AwardSearch, **defaults)


@pytest.fixture
def tool():
    return AwardLookupTool()


class TestLookupAward:
    async def test_exact_match_on_generated_unique_award_id(self, tool):
        award = await _make_award_search(generated_unique_award_id="CONT_AWD_W91278_9700_1234")

        result = await tool.lookup_award("CONT_AWD_W91278_9700_1234")

        assert result.results["award"]["generated_unique_award_id"] == award.generated_unique_award_id

    async def test_match_is_case_insensitive(self, tool):
        await _make_award_search(generated_unique_award_id="CONT_AWD_W91278_9700_1234")

        result = await tool.lookup_award("cont_awd_w91278_9700_1234")

        assert result.results["award"]["generated_unique_award_id"] == "CONT_AWD_W91278_9700_1234"

    async def test_exact_match_on_legacy_generated_unique_award_id(self, tool):
        await _make_award_search(
            generated_unique_award_id="CONT_AWD_NEW_123",
            generated_unique_award_id_legacy="CONT_AWD_LEGACY_123",
        )

        result = await tool.lookup_award("CONT_AWD_LEGACY_123")

        assert result.results["award"]["generated_unique_award_id"] == "CONT_AWD_NEW_123"

    async def test_exact_match_on_piid(self, tool):
        await _make_award_search(piid="W91CRB18D0001")

        result = await tool.lookup_award("W91CRB18D0001")

        assert result.results["award"]["piid"] == "W91CRB18D0001"

    async def test_exact_match_on_fain(self, tool):
        await _make_award_search(fain="ABCD1234")

        result = await tool.lookup_award("ABCD1234")

        assert result.results["award"]["fain"] == "ABCD1234"

    async def test_exact_match_on_uri(self, tool):
        await _make_award_search(uri="URI987654")

        result = await tool.lookup_award("URI987654")

        assert result.results["award"]["uri"] == "URI987654"

    async def test_returns_display_award_id_when_matched_by_generated_id(self, tool):
        await _make_award_search(
            generated_unique_award_id="CONT_AWD_W56HZV07C0096_9700_-NONE-_-NONE-",
            piid="W56HZV07C0096",
            display_award_id="W56HZV07C0096",
        )

        result = await tool.lookup_award("CONT_AWD_W56HZV07C0096_9700_-NONE-_-NONE-")

        assert result.results["award_id"] == "W56HZV07C0096"
        assert "W56HZV07C0096" in result.instructions

    async def test_award_id_falls_back_to_fain_without_display_award_id(self, tool):
        await _make_award_search(fain="F24AF01032", display_award_id=None)

        result = await tool.lookup_award("F24AF01032")

        assert result.results["award_id"] == "F24AF01032"

    async def test_query_is_stripped_before_matching(self, tool):
        await _make_award_search(piid="W91CRB18D0001")

        result = await tool.lookup_award("  W91CRB18D0001  ")

        assert result.results["award"]["piid"] == "W91CRB18D0001"

    async def test_empty_query_returns_error_without_raising(self, tool):
        result = await tool.lookup_award("   ")

        assert result.results["error"] == "Query cannot be empty"

    async def test_no_match_raises_award_not_recognized_error(self, tool):
        with pytest.raises(AwardNotRecognizedError, match="NOT_A_REAL_AWARD_ID"):
            await tool.lookup_award("NOT_A_REAL_AWARD_ID")

    async def test_no_match_error_points_to_lookup_recipient(self, tool):
        with pytest.raises(AwardNotRecognizedError) as exc_info:
            await tool.lookup_award("HR1JA12FSM63")

        assert "lookup_recipient" in exc_info.value.model_detail
        assert "lookup_recipient" not in exc_info.value.user_message


class TestLookupAwardToolRegistration:
    def test_tool_description_has_required_query_field(self):
        schema = lookup_award_tool.description.input_schema
        assert "query" in schema["properties"]
        assert schema["required"] == ["query"]

    def test_tool_function_is_bound_lookup_award(self):
        assert callable(lookup_award_tool.function)
