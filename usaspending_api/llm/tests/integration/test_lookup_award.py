import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from opensearchpy import ConnectionTimeout

from usaspending_api.llm.tools.exceptions import AwardNotRecognizedError, InvalidToolInputError
from usaspending_api.llm.tools.lookup_award import ES_LOOKUP_TIMEOUT_SECONDS, AwardLookupTool, lookup_award_tool
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

pytestmark = pytest.mark.django_db


@pytest.fixture
def award_index(monkeypatch, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    return elasticsearch_award_index


def _index_award(index, **fields):
    """Index an award document the way the loader stores it: ID fields uppercase."""
    doc = {"generated_unique_award_id": f"TEST_AWARD_{uuid.uuid4()}".upper(), **fields}
    index.client.index(index=index.index_name, body=doc)
    index.client.indices.refresh(index=index.index_name)
    return doc


@pytest.fixture
def tool():
    return AwardLookupTool()


class TestLookupAward:
    async def test_exact_match_on_generated_unique_award_id(self, tool, award_index):
        _index_award(award_index, generated_unique_award_id="CONT_AWD_W91278_9700_1234")

        result = await tool.lookup_award("CONT_AWD_W91278_9700_1234")

        assert result.results["award"]["generated_unique_award_id"] == "CONT_AWD_W91278_9700_1234"

    async def test_match_is_case_insensitive(self, tool, award_index):
        _index_award(award_index, generated_unique_award_id="CONT_AWD_W91278_9700_1234")

        result = await tool.lookup_award("cont_awd_w91278_9700_1234")

        assert result.results["award"]["generated_unique_award_id"] == "CONT_AWD_W91278_9700_1234"

    async def test_exact_match_on_piid(self, tool, award_index):
        _index_award(award_index, piid="W91CRB18D0001")

        result = await tool.lookup_award("w91crb18d0001")

        assert result.results["award"]["piid"] == "W91CRB18D0001"

    async def test_exact_match_on_fain(self, tool, award_index):
        _index_award(award_index, fain="ABCD1234")

        result = await tool.lookup_award("ABCD1234")

        assert result.results["award"]["fain"] == "ABCD1234"

    async def test_exact_match_on_uri(self, tool, award_index):
        _index_award(award_index, uri="URI987654")

        result = await tool.lookup_award("URI987654")

        assert result.results["award"]["uri"] == "URI987654"

    async def test_partial_id_does_not_match(self, tool, award_index):
        _index_award(award_index, piid="W91CRB18D0001")

        with pytest.raises(AwardNotRecognizedError):
            await tool.lookup_award("W91CRB18")

    async def test_returns_display_award_id_when_matched_by_generated_id(self, tool, award_index):
        _index_award(
            award_index,
            generated_unique_award_id="CONT_AWD_W56HZV07C0096_9700_-NONE-_-NONE-",
            piid="W56HZV07C0096",
            display_award_id="W56HZV07C0096",
        )

        result = await tool.lookup_award("CONT_AWD_W56HZV07C0096_9700_-NONE-_-NONE-")

        assert result.results["award_id"] == "W56HZV07C0096"
        assert "W56HZV07C0096" in result.instructions

    async def test_award_id_falls_back_to_fain_without_display_award_id(self, tool, award_index):
        _index_award(award_index, fain="F24AF01032")

        result = await tool.lookup_award("F24AF01032")

        assert result.results["award_id"] == "F24AF01032"

    async def test_generated_id_match_outranks_shared_piid_match(self, tool, award_index):
        _index_award(award_index, generated_unique_award_id="CONT_AWD_OTHER", piid="SHARED_ID")
        _index_award(award_index, generated_unique_award_id="SHARED_ID", piid="ANOTHER")

        result = await tool.lookup_award("SHARED_ID")

        assert result.results["award"]["generated_unique_award_id"] == "SHARED_ID"

    async def test_query_is_stripped_before_matching(self, tool, award_index):
        _index_award(award_index, piid="W91CRB18D0001")

        result = await tool.lookup_award("  W91CRB18D0001  ")

        assert result.results["award"]["piid"] == "W91CRB18D0001"

    async def test_empty_query_raises_invalid_input_with_instructions(self, tool):
        with pytest.raises(InvalidToolInputError, match="Query cannot be empty") as exc_info:
            await tool.lookup_award("   ")

        assert exc_info.value.instructions

    async def test_no_match_raises_award_not_recognized_error(self, tool, award_index):
        with pytest.raises(AwardNotRecognizedError, match="NOT_A_REAL_AWARD_ID"):
            await tool.lookup_award("NOT_A_REAL_AWARD_ID")

    async def test_no_match_error_points_to_lookup_recipient(self, tool, award_index):
        with pytest.raises(AwardNotRecognizedError) as exc_info:
            await tool.lookup_award("HR1JA12FSM63")

        assert "lookup_recipient" in exc_info.value.instructions
        assert "lookup_recipient" not in exc_info.value.user_message
        assert "lookup_recipient" not in exc_info.value.model_detail


class TestLookupAwardFailures:
    @staticmethod
    def _mock_search(**execute_kwargs):
        search = MagicMock()
        search.handle_execute = AsyncMock(**execute_kwargs)
        search.client.close = AsyncMock()
        return search

    async def test_connection_timeout_raises_unavailable_not_unrecognized(self, tool):
        search = self._mock_search(side_effect=ConnectionTimeout("TIMEOUT", "timed out", Exception()))

        with patch.object(AwardLookupTool, "_build_search", return_value=search):
            with pytest.raises(ConnectionTimeout):
                await tool.lookup_award("W91CRB18D0001")

        search.client.close.assert_awaited_once()

    async def test_server_side_timeout_is_not_reported_as_award_not_found(self, tool):
        search = self._mock_search(return_value=MagicMock(timed_out=True, hits=[]))

        with patch.object(AwardLookupTool, "_build_search", return_value=search):
            with pytest.raises(TimeoutError):
                await tool.lookup_award("W91CRB18D0001")

        search.client.close.assert_awaited_once()

        search.client.close.assert_awaited_once()

    async def test_search_uses_short_request_timeout(self, tool):
        search = AwardLookupTool._build_search("W91CRB18D0001")

        assert search._params["request_timeout"] == ES_LOOKUP_TIMEOUT_SECONDS == 10
        assert search.to_dict()["size"] == 1


class TestLookupAwardToolRegistration:
    def test_tool_description_has_required_query_field(self):
        schema = lookup_award_tool.description.input_schema
        assert "query" in schema["properties"]
        assert schema["required"] == ["query"]

    def test_tool_function_is_bound_lookup_award(self):
        assert callable(lookup_award_tool.function)
