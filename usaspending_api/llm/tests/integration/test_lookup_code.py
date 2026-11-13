from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from asgiref.sync import sync_to_async
from model_bakery import baker

from usaspending_api.accounts.models import FederalAccount
from usaspending_api.accounts.models.treasury_appropriation_account import TreasuryAppropriationAccount
from usaspending_api.llm.models.py_models import Filters
from usaspending_api.llm.tools.exceptions import (
    AssistanceListingNotRecognizedError,
    CodeNotRecognizedError,
    InvalidToolInputError,
    NaicsCodeNotRecognizedError,
    PscCodeNotRecognizedError,
    TasNotRecognizedError,
)
from usaspending_api.llm.tools.lookup_code import (
    CODE_TYPE_CONFIGS,
    CodeLookupTool,
    CodeResult,
    CodeTypeConfig,
    SearchResultNode,
    lookup_code_tool,
)
from usaspending_api.references.models import ToptierAgency
from usaspending_api.references.models.cfda import Cfda
from usaspending_api.references.models.naics import NAICS
from usaspending_api.references.models.psc import PSC

pytestmark = pytest.mark.django_db(transaction=True)


SAVE_NO_EMBED = {"auto_generate_embedding": False}


def _unit_vector(dimensions: int, index: int) -> list[float]:
    """Build a unit vector with a 1.0 at `index` so orthogonal vectors have
    cosine distance 1.0 (excluded by the 0.75 threshold) and identical
    vectors have cosine distance 0.0 (included)."""
    vec = [0.0] * dimensions
    vec[index] = 1.0
    return vec


async def _make_naics(code, description, embedding=None):
    naics = await sync_to_async(baker.make)(NAICS, code=code, description=description, _save_kwargs=SAVE_NO_EMBED)
    if embedding is not None:
        naics.embedding = embedding
        await sync_to_async(naics.save)(**SAVE_NO_EMBED)
    return naics


async def _make_psc(code, description):
    return await sync_to_async(baker.make)(
        PSC, code=code, description=description, length=len(code), _save_kwargs=SAVE_NO_EMBED
    )


async def _make_cfda(program_number, program_title, popular_name=None):
    return await sync_to_async(baker.make)(
        Cfda,
        program_number=program_number,
        program_title=program_title,
        popular_name=popular_name,
        _save_kwargs=SAVE_NO_EMBED,
    )


async def _make_tas(
    label,
    title,
    agency_id="012",
    main="0123",
    sub="000",
    ata=None,
    bpoa=None,
    epoa=None,
    embedding=None,
    toptier_code=None,
    with_file_d_data=True,
    agency_name=None,
    agency_abbreviation=None,
):
    """Make a TAS under the federal account of its AID and main account, listed under the toptier agency with
    toptier_code (the AID unless given, as an AID is not always a toptier code). Like the TAS filter tree, the lookup
    only returns a TAS that has award data (File D)."""

    def make():
        toptier_code_ = toptier_code or agency_id
        agency = ToptierAgency.objects.filter(toptier_code=toptier_code_).first() or baker.make(
            ToptierAgency,
            toptier_code=toptier_code_,
            name=agency_name or f"Agency {toptier_code_}",
            abbreviation=agency_abbreviation or f"A{toptier_code_}",
            _save_kwargs=SAVE_NO_EMBED,
        )
        federal_account = FederalAccount.objects.filter(
            agency_identifier=agency_id, main_account_code=main
        ).first() or baker.make(
            FederalAccount,
            agency_identifier=agency_id,
            main_account_code=main,
            federal_account_code=f"{agency_id}-{main}",
            account_title=title,
            parent_toptier_agency=agency,
        )
        tas = baker.make(
            TreasuryAppropriationAccount,
            tas_rendering_label=label,
            account_title=title,
            agency_id=agency_id,
            main_account_code=main,
            sub_account_code=sub,
            allocation_transfer_agency_id=ata,
            beginning_period_of_availability=bpoa,
            ending_period_of_availability=epoa,
            federal_account=federal_account,
            _save_kwargs=SAVE_NO_EMBED,
        )
        if with_file_d_data:
            baker.make("search.AwardSearch", award_id=tas.pk)
            baker.make("awards.FinancialAccountsByAwards", treasury_account=tas, award_id=tas.pk)
        if embedding is not None:
            tas.embedding = embedding
            tas.save(**SAVE_NO_EMBED)
        return tas

    return await sync_to_async(make)()


@pytest.fixture
def mock_embedding_generator():
    """Mock EmbeddingGenerator so no real embedding API calls are made.
    Returns the mock instance so tests can control agenerate_embedding output."""
    with patch("usaspending_api.llm.tools.lookup_code.EmbeddingGenerator") as mock_cls:
        mock_instance = MagicMock()
        mock_instance.agenerate_embedding = AsyncMock()
        mock_cls.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_expand_query():
    """By default, disable fanout expansion so hybrid search runs against
    only the original query string."""
    with patch("usaspending_api.llm.tools.lookup_code.expand_query", new_callable=AsyncMock) as mock_fn:
        mock_fn.side_effect = lambda query, model, query_fanout: [query]
        yield mock_fn


@pytest.fixture
def mock_get_aimodel():
    with patch("usaspending_api.llm.tools.lookup_code.AIModel.objects.aget", new_callable=AsyncMock) as mock_fn:
        mock_fn.return_value = MagicMock()
        yield mock_fn


@pytest.fixture
def tool():
    return CodeLookupTool()


def _walk(nodes):
    for node in nodes:
        yield node
        yield from _walk(node.get("children", []))


def _tree_nodes(result) -> dict[str, dict]:
    """Every node in a lookup_codes result's tree, by code."""
    return {node["code"]: node for node in _walk(result.results["tree"])}


def _matched_codes(result) -> set[str]:
    """The codes in a lookup_codes result's tree that the search matched, as opposed to ancestors shown for context."""
    return {code for code, node in _tree_nodes(result).items() if node["matched"]}


class TestQueryExactAndPrefixMatches:
    async def test_exact_match_case_insensitive(self, tool):
        await _make_naics("541330", "Test construction")
        config = CODE_TYPE_CONFIGS["naics"]

        exact_qs, prefix_qs = tool._query_exact_and_prefix_matches(config, "541330")
        assert [row async for row in exact_qs.values_list("code", flat=True)] == ["541330"]

    async def test_prefix_match_excludes_exact(self, tool):
        await _make_naics("5413", "Parent")
        await _make_naics("541330", "Child")
        config = CODE_TYPE_CONFIGS["naics"]

        exact_qs, prefix_qs = tool._query_exact_and_prefix_matches(config, "5413")
        assert [row async for row in exact_qs.values_list("code", flat=True)] == ["5413"]
        assert [row async for row in prefix_qs.values_list("code", flat=True)] == ["541330"]

    async def test_no_matches_returns_empty_querysets(self, tool):
        config = CODE_TYPE_CONFIGS["naics"]
        exact_qs, prefix_qs = tool._query_exact_and_prefix_matches(config, "999999")
        assert not await exact_qs.aexists()
        assert not await prefix_qs.aexists()


class TestExactMatchShortCircuit:
    async def test_exact_match_skips_embeddings_and_fanout(self, tool, mock_embedding_generator, mock_expand_query):
        await _make_naics("541330", "Construction of buildings")

        result = await tool.lookup_codes("541330", "naics", query_fanout=3)

        assert _matched_codes(result) == {"541330"}
        mock_embedding_generator.agenerate_embedding.assert_not_called()
        mock_expand_query.assert_not_called()

    async def test_exact_match_score_is_one(self, tool, mock_embedding_generator, mock_expand_query):
        await _make_naics("541330", "Construction of buildings")

        config = CODE_TYPE_CONFIGS["naics"]
        result = await tool._handle_exact_search("541330", config, "naics", 20)
        node = result["hierarchy"]["541330"]
        assert node["score"] == 1.0

    async def test_exact_match_case_insensitive_input(self, tool, mock_embedding_generator, mock_expand_query):
        await _make_cfda("10.557", "Nutrition Program")

        result = await tool.lookup_codes("10.557", "cfda")
        assert _matched_codes(result) == {"10.557"}


class TestPrefixMatch:
    async def test_prefix_match_returns_all_matching_codes(self, tool, mock_embedding_generator, mock_expand_query):
        await _make_naics("541330", "Construction of buildings")
        await _make_naics("541340", "Drafting services")
        await _make_naics("541511", "Custom computer programming")

        result = await tool.lookup_codes("5413", "naics")

        codes_in_tree = set(_tree_nodes(result))
        assert "541330" in codes_in_tree
        assert "541340" in codes_in_tree
        assert "541511" not in codes_in_tree

    async def test_prefix_match_skips_embeddings(self, tool, mock_embedding_generator, mock_expand_query):
        await _make_naics("541330", "Construction of buildings")

        await tool.lookup_codes("5413", "naics")

        mock_embedding_generator.agenerate_embedding.assert_not_called()
        mock_expand_query.assert_not_called()

    async def test_prefix_match_score_below_exact(self, tool, mock_embedding_generator, mock_expand_query):
        await _make_naics("541330", "Construction of buildings")

        config = CODE_TYPE_CONFIGS["naics"]
        result = await tool._handle_exact_search("5413", config, "naics", 20)
        node = result["hierarchy"]["541330"]
        assert node["score"] == 0.95


class TestHybridSearch:
    async def test_falls_through_to_embedding_search(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel
    ):
        dims = getattr(NAICS, "embedding_dimensions", 256)
        await _make_naics("999999", "Totally unrelated description", embedding=_unit_vector(dims, 0))

        mock_embedding_generator.agenerate_embedding.return_value = _unit_vector(dims, 0)

        result = await tool.lookup_codes("some semantic query", "naics")

        assert "999999" in _matched_codes(result)
        mock_embedding_generator.agenerate_embedding.assert_called()

    async def test_vector_distance_threshold_excludes_far_matches(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel
    ):
        dims = getattr(NAICS, "embedding_dimensions", 256)

        await _make_naics("111111", "Close match", embedding=_unit_vector(dims, 0))
        await _make_naics("222222", "Far match", embedding=_unit_vector(dims, 1))  # orthogonal -> distance 1.0

        mock_embedding_generator.agenerate_embedding.return_value = _unit_vector(dims, 0)

        result = await tool.lookup_codes("query text", "naics")
        codes = set(_tree_nodes(result))

        assert "111111" in codes
        assert "222222" not in codes

    async def test_top_k_truncates_results(self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel):
        dims = getattr(NAICS, "embedding_dimensions", 256)
        mock_embedding_generator.agenerate_embedding.return_value = _unit_vector(dims, 0)

        for i in range(5):
            await _make_naics(f"10000{i}", f"Match {i}", embedding=_unit_vector(dims, 0))

        result = await tool.lookup_codes("query text", "naics", top_k=2)
        assert len(_matched_codes(result)) <= 2

    async def test_use_fanout_false_does_not_call_expand_query(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel
    ):
        dims = getattr(NAICS, "embedding_dimensions", 256)
        await _make_naics("999999", "Matching description", embedding=_unit_vector(dims, 0))
        mock_embedding_generator.agenerate_embedding.return_value = _unit_vector(dims, 0)

        await tool.lookup_codes("query text", "naics", query_fanout=None)

        mock_expand_query.assert_not_called()

    async def test_embedding_failure_raises_not_recognized(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel
    ):
        mock_embedding_generator.agenerate_embedding.side_effect = Exception("API down")

        with pytest.raises(NaicsCodeNotRecognizedError, match="query text"):
            await tool.lookup_codes("query text", "naics")

    async def test_duplicate_matches_across_fanout_keep_higher_score(
        self, tool, mock_embedding_generator, mock_get_aimodel
    ):
        dims = getattr(NAICS, "embedding_dimensions", 256)
        await _make_naics("333333", "Some match", embedding=_unit_vector(dims, 0))

        with patch("usaspending_api.llm.tools.lookup_code.expand_query", new_callable=AsyncMock) as mock_expand:
            mock_expand.return_value = ["variation a", "variation b"]
            mock_embedding_generator.agenerate_embedding.return_value = _unit_vector(dims, 0)

            result = await tool.lookup_codes("query text", "naics", query_fanout=3)

        assert "333333" in _matched_codes(result)


class TestFinalizeResultsHierarchy:
    async def test_ancestors_are_pulled_in_and_nested(self, tool, monkeypatch):
        await _make_naics("54", "Professional services")
        await _make_naics("5413", "Architectural/engineering")
        await _make_naics("541330", "Engineering services")

        parent_map = {"541330": "5413", "5413": "54"}
        ancestor_map = {"541330": ["5413", "54"], "5413": ["54"], "54": []}

        config = CODE_TYPE_CONFIGS["naics"]
        monkeypatch.setattr(config, "get_parent_code", lambda code: parent_map.get(code))
        monkeypatch.setattr(config, "get_all_ancestors", lambda code: ancestor_map.get(code, []))

        all_results = {"541330": CodeResult(code="541330", description="Engineering services", score=1.0)}

        result = await tool._finalize_results(all_results, config, "naics", top_k=20)

        assert "54" in result["hierarchy"]
        assert "5413" in result["hierarchy"]["54"]["children"]
        assert "541330" in result["hierarchy"]["54"]["children"]["5413"]["children"]
        assert result["total_codes"] == 3

    async def test_multiple_roots_sorted_by_best_score_in_tree(self, tool, monkeypatch):
        config = CODE_TYPE_CONFIGS["naics"]
        monkeypatch.setattr(config, "get_parent_code", lambda code: {"B2": "B1"}.get(code))
        monkeypatch.setattr(config, "get_all_ancestors", lambda code: [])

        all_results = {
            "A1": CodeResult(code="A1", description="Low root", score=0.2),
            "B1": CodeResult(code="B1", description="High root parent", score=0.1),
            "B2": CodeResult(code="B2", description="High score child", score=0.9),
        }

        result = await tool._finalize_results(all_results, config, "naics", top_k=20)

        roots = list(result["hierarchy"].keys())
        assert roots[0] == "B1"  # best descendant score (0.9) wins over A1 (0.2)
        assert roots[1] == "A1"

    async def test_top_k_applied_before_ancestor_lookup(self, tool, monkeypatch):
        config = CODE_TYPE_CONFIGS["naics"]
        monkeypatch.setattr(config, "get_parent_code", lambda code: None)
        monkeypatch.setattr(config, "get_all_ancestors", lambda code: [])

        all_results = {f"C{i}": CodeResult(code=f"C{i}", description=f"desc {i}", score=float(i)) for i in range(5)}

        result = await tool._finalize_results(all_results, config, "naics", top_k=2)

        assert result["total_codes"] == 2
        assert set(result["hierarchy"].keys()) == {"C4", "C3"}

    async def test_empty_results_returns_empty_hierarchy(self, tool):
        config = CODE_TYPE_CONFIGS["naics"]
        result = await tool._finalize_results({}, config, "naics", top_k=20)
        assert result == {"hierarchy": {}, "total_codes": 0}

    async def test_non_psc_code_type_does_not_apply_tier1_grouping(self, tool, monkeypatch):
        """The synthetic Tier1 grouping step is PSC-only; other code types keep their real root
        codes as top-level hierarchy keys."""
        config = CODE_TYPE_CONFIGS["naics"]
        monkeypatch.setattr(config, "get_parent_code", lambda code: None)
        monkeypatch.setattr(config, "get_all_ancestors", lambda code: [])

        all_results = {"33": CodeResult(code="33", description="Manufacturing", score=0.5)}
        result = await tool._finalize_results(all_results, config, "naics", top_k=20)

        assert set(result["hierarchy"].keys()) == {"33"}


class TestGroupPscRootsByTier1:
    """Tests for CodeLookupTool._group_psc_roots_by_tier1, the PSC-only post-processing step that
    wraps each real PSC root ('A', 'D', '10', ...) under its synthetic Tier1 PSC_GROUPS label
    (Research and Development / Service / Product), matching the grouping the real PSC filter tree
    API uses (PSCFilterTree.toptier_search).
    """

    @staticmethod
    def _root_node(code, score=0.5, children=None):
        return {
            "code": code,
            "description": f"description for {code}",
            "score": score,
            "children": children or {},
        }

    def test_groups_roots_under_their_tier1_labels(self):
        hierarchy = {
            "A": self._root_node("A"),
            "D": self._root_node("D"),
            "10": self._root_node("10"),
        }

        grouped = CodeLookupTool._group_psc_roots_by_tier1(hierarchy)

        assert set(grouped.keys()) == {"Research and Development", "Service", "Product"}

    def test_research_and_development_merges_root_children_without_nesting_the_root_itself(self):
        """'A' is the only PSC code whose root value is itself a valid description in the PSC
        table, so its children are merged straight into the Tier1 bucket instead of nesting 'A'
        as an intermediate child node, unlike the Service/Product groups."""
        hierarchy = {"A": self._root_node("A", children={"AC": self._root_node("AC", children=None)})}

        grouped = CodeLookupTool._group_psc_roots_by_tier1(hierarchy)

        rd_bucket = grouped["Research and Development"]
        assert "A" not in rd_bucket["children"]
        assert "AC" in rd_bucket["children"]

    def test_research_and_development_group_is_flagged_when_the_a_root_itself_matched(self):
        hierarchy = {"A": self._root_node("A", score=0.98, children={"AC": self._root_node("AC", score=0.2)})}

        grouped = CodeLookupTool._group_psc_roots_by_tier1(hierarchy)

        assert grouped["Research and Development"]["is_group_match"] is True

    def test_research_and_development_group_is_not_flagged_when_the_a_root_is_only_an_ancestor(self):
        hierarchy = {"A": self._root_node("A", score=0.0, children={"AC": self._root_node("AC", score=0.9)})}

        grouped = CodeLookupTool._group_psc_roots_by_tier1(hierarchy)

        assert grouped["Research and Development"]["is_group_match"] is False

    def test_bucket_score_is_the_best_score_among_its_roots(self):
        hierarchy = {
            "D": self._root_node("D", score=0.2),
            "F": self._root_node("F", score=0.8),
        }

        grouped = CodeLookupTool._group_psc_roots_by_tier1(hierarchy)

        assert grouped["Service"]["score"] == 0.8

    def test_bucket_description_is_empty_string(self):
        hierarchy = {"D": self._root_node("D")}

        grouped = CodeLookupTool._group_psc_roots_by_tier1(hierarchy)

        assert grouped["Service"]["description"] == ""


class TestSearchResultNode:
    def test_to_dict_shape_and_child_sorting(self):
        config = CodeTypeConfig(
            model_class=NAICS,
            code_field="code",
            description_field="description",
            get_parent_code=lambda code: {"5413": "54", "5411": "54"}.get(code),
            get_all_ancestors=lambda code: [],
        )

        all_results = {
            "54": CodeResult(code="54", description="Root", score=0.1),
            "5413": CodeResult(code="5413", description="High child", score=0.9),
            "5411": CodeResult(code="5411", description="Low child", score=0.3),
        }

        node = SearchResultNode(code="54", description="Root", score=0.1, all_results=all_results, config=config)
        d = node.to_dict()

        assert set(d.keys()) == {"code", "description", "score", "children"}
        assert list(d["children"].keys()) == ["5413", "5411"]  # sorted by score desc

    def test_no_children_when_config_has_no_parent_fn(self):
        config = CodeTypeConfig(
            model_class=Cfda,
            code_field="program_number",
            description_field="program_title",
            get_parent_code=None,
            get_all_ancestors=lambda code: [],
        )
        all_results = {"10.557": CodeResult(code="10.557", description="Program", score=1.0)}

        node = SearchResultNode(code="10.557", description="Program", score=1.0, all_results=all_results, config=config)
        assert node.children == []


class TestCfdaPopularName:
    async def test_popular_name_is_part_of_the_filter_value(self, tool):
        await _make_cfda(
            "10.001", "Agricultural Research Basic and Applied Research", popular_name="Extramural Research"
        )

        result = await tool.lookup_codes("10.001", "cfda")

        node = _tree_nodes(result)["10.001"]
        assert node["popular_name"] == "Extramural Research"
        assert node["filter_value"] == {
            "identifier": "10.001",
            "program_number": "10.001",
            "program_title": "Agricultural Research Basic and Applied Research",
            "popular_name": "Extramural Research",
        }


class TestCfdaWithoutPopularName:
    @pytest.mark.parametrize("popular_name", [None, ""])
    async def test_empty_popular_name_is_an_empty_string(self, tool, popular_name):
        await _make_cfda("14.257", "Prevention", popular_name=popular_name)

        result = await tool.lookup_codes("14.257", "cfda")

        assert _tree_nodes(result)["14.257"]["filter_value"] == {
            "identifier": "14.257",
            "program_number": "14.257",
            "program_title": "Prevention",
            "popular_name": "",
        }


class TestOneCodePerLookup:
    """A code is looked up on its own. Several codes in one query are not read as a list of codes, which keeps the
    llm from combining codes of different types in a single lookup."""

    async def test_query_that_lists_codes_is_not_split(self, tool, mock_embedding_generator, mock_get_aimodel):
        mock_embedding_generator.agenerate_embedding.return_value = None
        await _make_naics("11", "Agriculture, Forestry, Fishing and Hunting")
        await _make_naics("1111", "Oilseed and Grain Farming")
        await _make_naics("111110", "Soybean Farming")
        await _make_naics("111120", "Oilseed Farming")

        with pytest.raises(NaicsCodeNotRecognizedError) as exc_info:
            await tool.lookup_codes("111110, 111120", "naics", query_fanout=None)

        assert "111110, 111120" in exc_info.value.model_detail

    async def test_each_code_is_found_by_its_own_lookup(self, tool):
        await _make_naics("11", "Agriculture, Forestry, Fishing and Hunting")
        await _make_naics("1111", "Oilseed and Grain Farming")
        await _make_naics("111110", "Soybean Farming")
        await _make_naics("111120", "Oilseed Farming")

        first = await tool.lookup_codes("111110", "naics")
        second = await tool.lookup_codes("111120", "naics")

        assert _matched_codes(first) == {"111110"}
        assert _matched_codes(second) == {"111120"}


class TestResearchAndDevelopmentPscTree:
    """The client has a three character node (AA1) above the research and development codes that start the same way
    (AA11, AA12, ...). It takes the description of AA10, and there is no AA10 for the client."""

    @pytest.fixture(autouse=True)
    async def codes(self):
        await _make_psc("A", "Research and Development")
        await _make_psc("AA", "Agriculture R&D")
        await _make_psc("AA10", "Agriculture R&D Basic Research")
        await _make_psc("AA11", "Agriculture R&D Applied Research")
        await _make_psc("AA12", "Agriculture R&D Development")

    async def test_codes_are_under_the_three_character_node(self, tool):
        result = await tool.lookup_codes("AA11", "psc")

        nodes = _tree_nodes(result)
        assert nodes["AA11"]["filter_value"] == ["Research and Development", "AA", "AA1", "AA11"]
        assert nodes["AA1"]["filter_value"] == ["Research and Development", "AA", "AA1"]
        assert nodes["AA"]["filter_value"] == ["Research and Development", "AA"]

    async def test_there_is_no_code_ending_in_0(self, tool):
        result = await tool.lookup_codes("AA1", "psc")

        nodes = _tree_nodes(result)
        assert "AA10" not in nodes
        # The node has the description of AA10, and covers the codes beneath it but not AA10 itself
        assert nodes["AA1"]["description"] == "Agriculture R&D Basic Research"
        assert nodes["AA1"]["covers"] == 2
        assert nodes["AA1"]["matched"]

    async def test_query_for_a_code_ending_in_0_is_the_three_character_node(self, tool):
        result = await tool.lookup_codes("AA10", "psc")

        nodes = _tree_nodes(result)
        assert "AA10" not in nodes
        assert nodes["AA1"]["filter_value"] == ["Research and Development", "AA", "AA1"]
        assert "The query is itself a code" in result.instructions

    async def test_two_character_code_covers_all_of_its_codes(self, tool):
        result = await tool.lookup_codes("AA", "psc")

        assert _tree_nodes(result)["AA"]["covers"] == 3


class TestTasTopK:
    async def test_top_k_counts_accounts_not_periods_of_availability(self, tool):
        for year in ("2019", "2020", "2021"):
            await _make_tas(
                f"012-{year}/{year}-0102-000", "Account A", agency_id="012", main="0102", bpoa=year, epoa=year
            )
        await _make_tas("014-X-0102-000", "Account B", agency_id="014", main="0102")
        await _make_tas("310-X-0102-000", "Account C", agency_id="310", main="0102")

        result = await tool.lookup_codes("0102", "tas", top_k=2)

        # The three periods of the 012 account don't use up the limit, so a second account is returned
        assert {code for code in _tree_nodes(result) if code.endswith("-0102")} == {"012-0102", "014-0102"}


class TestTasAgencyQueries:
    """A query that names an agency matches the agency, which stands for all of its accounts."""

    @pytest.fixture(autouse=True)
    async def accounts(self):
        treasury = {"agency_name": "Department of the Treasury", "agency_abbreviation": "TREAS", "toptier_code": "020"}
        await _make_tas(
            "020-X-6104-000", "Albanian Claims Fund, Treasury (T)", agency_id="020", main="6104", **treasury
        )
        await _make_tas("020-X-0150-000", "Treasury Forfeiture Fund", agency_id="020", main="0150", **treasury)
        await _make_tas("001-X-5760-000", "Capitol Trust Account", agency_id="001", main="5760", toptier_code="001")
        # An agency with accounts that have no award data can't be chosen
        await _make_tas(
            "097-X-0001-000",
            "Defense Account",
            agency_id="097",
            main="0001",
            agency_name="Department of Defense",
            agency_abbreviation="DOD",
            toptier_code="097",
            with_file_d_data=False,
        )

    @pytest.mark.parametrize(
        "query", ["treasury account", "treasury accounts", "Treasury", "TREAS", "Department of the Treasury"]
    )
    async def test_agency_is_the_match(self, tool, query):
        result = await tool.lookup_codes(query, "tas")

        assert len(result.results["tree"]) == 1
        agency = result.results["tree"][0]
        assert (agency["code"], agency["matched"], agency["filter_value"]) == ("020", True, ["020"])
        assert agency["description"] == "Department of the Treasury (TREAS)"
        assert agency["covers"] == 2
        assert "children" not in agency
        assert "The query names an agency" in result.instructions
        assert "lookup_agencies" in result.instructions

    async def test_agency_without_award_data_is_not_matched(self, tool, mock_embedding_generator, mock_get_aimodel):
        mock_embedding_generator.agenerate_embedding.return_value = None

        with pytest.raises(TasNotRecognizedError):
            await tool.lookup_codes("defense accounts", "tas", query_fanout=None)

    async def test_query_naming_more_than_the_agency_is_not_an_agency_match(
        self, tool, mock_embedding_generator, mock_get_aimodel
    ):
        mock_embedding_generator.agenerate_embedding.return_value = None

        with pytest.raises(TasNotRecognizedError):
            await tool.lookup_codes("treasury forfeiture fund", "tas", query_fanout=None)


class TestTasLineage:
    """A TAS is listed under the agency and federal account of its federal account, as in the TAS filter tree, which is
    not always the AID in its label (an AID of 011 is listed under the toptier agency 1100)."""

    @pytest.fixture(autouse=True)
    async def accounts(self):
        await _make_tas("011-X-0001-000", "Presidential Account", agency_id="011", main="0001", toptier_code="1100")
        await _make_tas("012-X-0002-000", "Ordinary Account", agency_id="012", main="0002")
        await _make_tas("012-X-0003-000", "No Award Data", agency_id="012", main="0003", with_file_d_data=False)

    async def test_tas_is_under_the_agency_of_its_federal_account(self, tool):
        result = await tool.lookup_codes("011-0001", "tas")

        nodes = _tree_nodes(result)
        assert result.results["tree"][0]["code"] == "1100"
        assert nodes["1100"]["description"] == "Agency 1100 (A1100)"
        assert nodes["011-0001"]["filter_value"] == ["1100", "011-0001"]
        assert "011" not in nodes

        result = await tool.lookup_codes("011-X-0001-000", "tas")
        assert _tree_nodes(result)["011-X-0001-000"]["filter_value"] == ["1100", "011-0001", "011-X-0001-000"]

    async def test_federal_account_is_described_by_its_title(self, tool):
        result = await tool.lookup_codes("012-0002", "tas")

        assert _tree_nodes(result)["012-0002"]["description"] == "Ordinary Account"

    async def test_toptier_code_finds_the_agency_whatever_the_aid(self, tool):
        result = await tool.lookup_codes("1100", "tas")

        assert [node["code"] for node in result.results["tree"]] == ["1100"]
        assert result.results["tree"][0]["matched"]

    async def test_aid_finds_the_tas_under_their_toptier_agency(self, tool):
        result = await tool.lookup_codes("011", "tas")

        assert "011-X-0001-000" in _matched_codes(result)
        assert result.results["tree"][0]["code"] == "1100"

    async def test_tas_without_award_data_is_not_returned(self, tool, mock_embedding_generator, mock_get_aimodel):
        mock_embedding_generator.agenerate_embedding.return_value = None

        with pytest.raises(TasNotRecognizedError):
            await tool.lookup_codes("012-X-0003-000", "tas", query_fanout=None)

    async def test_covers_counts_the_tas_with_award_data(self, tool):
        await _make_tas("012-2020/2021-0002-000", "Ordinary Account", agency_id="012", main="0002")
        await _make_tas("012-2021/2022-0002-000", "Ordinary Account", agency_id="012", main="0002")

        result = await tool.lookup_codes("012", "tas")
        nodes = _tree_nodes(result)
        assert nodes["012"]["covers"] == 3
        assert nodes["012"].get("children") is None

        result = await tool.lookup_codes("012-0002", "tas")
        assert _tree_nodes(result)["012-0002"]["covers"] == 3

        result = await tool.lookup_codes("012-2020/2021-0002-000", "tas")
        assert _tree_nodes(result)["012-2020/2021-0002-000"]["covers"] == 1

    async def test_tas_path_copied_from_the_lookup_is_kept(self):
        filters = await sync_to_async(Filters)(tasCodes={"require": [["1100", "011-0001", "011-X-0001-000"]]})

        assert filters.tasCodes.require == [["1100", "011-0001", "011-X-0001-000"]]

    async def test_path_of_a_tas_comes_from_its_federal_account(self):
        from usaspending_api.llm.tools.helpers.code_paths import resolve_tas_path

        assert await sync_to_async(resolve_tas_path)("011-X-0001-000") == ["1100", "011-0001", "011-X-0001-000"]
        assert await sync_to_async(resolve_tas_path)("999-X-0001-000") is None


class TestErrorHandling:
    async def test_unsupported_code_type_raises_invalid_input(self, tool):
        with pytest.raises(
            InvalidToolInputError,
            match=r"Unsupported code type: bogus. Supported types: \['naics', 'psc', 'cfda', 'tas'\]",
        ) as exc_info:
            await tool.lookup_codes("anything", "bogus")
        assert exc_info.value.instructions

    async def test_no_matches_anywhere_raises_not_recognized(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel
    ):
        mock_embedding_generator.agenerate_embedding.return_value = None

        with pytest.raises(NaicsCodeNotRecognizedError, match="nothing matches this"):
            await tool.lookup_codes("nothing matches this", "naics")

    @pytest.mark.parametrize(
        "code_type, exception_class",
        [
            ("naics", NaicsCodeNotRecognizedError),
            ("psc", PscCodeNotRecognizedError),
            ("cfda", AssistanceListingNotRecognizedError),
            ("tas", TasNotRecognizedError),
        ],
    )
    async def test_no_matches_raises_code_type_specific_error(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel, code_type, exception_class
    ):
        mock_embedding_generator.agenerate_embedding.return_value = None

        with pytest.raises(exception_class):
            await tool.lookup_codes("nothing matches this", code_type)

    @pytest.mark.parametrize("code_type", ["naics", "psc", "cfda", "tas"])
    async def test_no_matches_error_keeps_detail_and_adds_instructions(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel, code_type
    ):
        mock_embedding_generator.agenerate_embedding.return_value = None

        with pytest.raises(CodeNotRecognizedError) as exc_info:
            await tool.lookup_codes("nothing matches this", code_type)

        assert "nothing matches this" in exc_info.value.model_detail
        assert exc_info.value.instructions
        assert ("Do not retry the same string" in exc_info.value.instructions) == (code_type in ("cfda", "tas"))
        assert "tell the user" not in exc_info.value.instructions
        assert ("dates" in exc_info.value.instructions) == (code_type == "tas")


def _node(score, children=None):
    return {"score": score, "description": "", "children": children or {}}


class TestNormalizeQuery:
    @pytest.mark.parametrize(
        "query, code_type, expected",
        [
            ("AL 14.257", "cfda", "14.257"),
            ("al 14.257", "cfda", "14.257"),
            ("ALN 14.257", "cfda", "14.257"),
            ("CFDA: 14.257", "cfda", "14.257"),
            ("Assistance Listing #14.257", "cfda", "14.257"),
            ("assistance listing number 93.00A", "cfda", "93.00A"),
            ("grants for program 93.HDN please", "cfda", "93.HDN"),
            ("14.257", "cfda", "14.257"),
            ("AL14.257", "cfda", "14.257"),
            ("grants under program 14.257 please", "cfda", "14.257"),
            # A program number is only pulled out of a phrase when there is exactly one
            ("14.257 and 14.258", "cfda", "14.257 and 14.258"),
            ("AL 14", "cfda", "14"),
            ("homelessness", "cfda", "homelessness"),
            ("NAICS 1111", "naics", "1111"),
            ("naics code 111110", "naics", "111110"),
            ("PSC AA11", "psc", "AA11"),
            ("TAS 310-2012/2012-3200-000", "tas", "310-2012/2012-3200-000"),
            ("Treasury Account Symbol 310", "tas", "310"),
            # Labels followed by words, not a code, are left alone
            ("PSC for software", "psc", "PSC for software"),
            ("assistance listing for housing", "cfda", "assistance listing for housing"),
            ("  541330  ", "naics", "541330"),
        ],
    )
    def test_query_is_reduced_to_the_code(self, tool, query, code_type, expected):
        assert tool._normalize_query(query, code_type) == expected

    async def test_labelled_assistance_listing_is_found_by_exact_match(
        self, tool, mock_embedding_generator, mock_expand_query
    ):
        await _make_cfda("14.257", "Homelessness Prevention and Rapid Re-Housing Program")

        result = await tool.lookup_codes("AL 14.257", "cfda")

        assert _matched_codes(result) == {"14.257"}
        mock_embedding_generator.agenerate_embedding.assert_not_called()


class TestMarkTasAccountsMatched:
    def test_unscored_accounts_are_matched_but_agencies_and_scored_codes_are_left_alone(self, tool):
        hierarchy = {"310": _node(0.0, {"310-3200": _node(0.0, {"310-X-3200-000": _node(0.95)})})}

        marked = tool._mark_tas_accounts_matched(hierarchy)

        assert marked["310"]["score"] == 0.0
        assert marked["310"]["children"]["310-3200"]["score"] == 0.95
        assert marked["310"]["children"]["310-3200"]["children"]["310-X-3200-000"]["score"] == 0.95

    @pytest.mark.parametrize(
        "query, expected", [("3200", True), ("014-0102", True), ("treasury", False), ("310", False)]
    )
    def test_account_queries(self, tool, query, expected):
        assert tool._is_tas_account_query(query) is expected


class TestKeepOnlyPathTo:
    def test_keeps_the_ancestors_and_drops_everything_beneath_the_code(self, tool):
        hierarchy = {
            "23": _node(0.0, {"2362": _node(1.0, {"236220": _node(0.95)}), "2361": _node(0.0)}),
            "33": _node(0.0),
        }

        pruned = tool._keep_only_path_to(hierarchy, "2362")

        assert list(pruned) == ["23"]
        assert list(pruned["23"]["children"]) == ["2362"]
        assert pruned["23"]["children"]["2362"]["children"] == {}

    def test_code_that_was_only_an_ancestor_counts_as_matched(self, tool):
        pruned = tool._keep_only_path_to({"309": _node(0.0, {"309-0200": _node(0.0)})}, "309")

        assert pruned["309"]["score"] == 1.0

    def test_unknown_code_leaves_nothing(self, tool):
        assert tool._keep_only_path_to({"23": _node(0.0)}, "99") == {}


class TestBuildOutputNode:
    def test_matched_and_context_nodes_are_labelled(self, tool):
        node = _node(0.0, {"3315": _node(0.0, {"331511": _node(0.954)})})

        output = tool._build_output_node("33", node, "naics", {"33": 12, "3315": 4, "331511": 1})

        assert (output["code"], output["matched"], output["score"], output["covers"]) == ("33", False, 0.0, 12)
        leaf = output["children"][0]["children"][0]
        assert (leaf["code"], leaf["matched"], leaf["score"], leaf["filter_value"]) == ("331511", True, 0.95, "331511")
        assert "children" not in leaf

    def test_children_are_ordered_by_the_best_score_beneath_them(self, tool):
        node = _node(0.0, {"A": _node(0.2), "B": _node(0.0, {"B1": _node(0.9)}), "C": _node(0.5)})

        output = tool._build_output_node("R", node, "naics", {})

        assert [child["code"] for child in output["children"]] == ["B", "C", "A"]

    def test_psc_group_label_is_left_out_and_its_children_take_its_place(self, tool):
        hierarchy = {
            "Service": _node(0.9, {"D": _node(0.9)}),
            "Product": _node(0.4, {"10": _node(0.4)}),
        }

        roots = tool._order_output_nodes(tool._rank_output_nodes(hierarchy, "psc", {}))

        assert [root["code"] for root in roots] == ["D", "10"]
        assert [root["filter_value"] for root in roots] == [["Service", "D"], ["Product", "10"]]

    def test_matched_research_and_development_group_is_kept(self, tool):
        node = {**_node(0.98, {"AC": _node(0.2)}), "is_group_match": True}

        roots = tool._order_output_nodes(tool._rank_output_nodes({"Research and Development": node}, "psc", {}))

        assert [root["code"] for root in roots] == ["Research and Development"]
        assert roots[0]["matched"] is True
        assert roots[0]["filter_value"] == ["Research and Development"]
        assert [child["code"] for child in roots[0]["children"]] == ["AC"]

    def test_assistance_listing_agency_has_no_filter_value_but_a_program_does(self, tool):
        agency = tool._build_output_node("14", _node(0.0), "cfda", {})
        program = tool._build_output_node("14.257", {**_node(0.8), "description": "Prevention"}, "cfda", {})

        assert "filter_value" not in agency
        assert program["filter_value"] == {
            "identifier": "14.257",
            "program_number": "14.257",
            "program_title": "Prevention",
        }

    def test_only_the_best_scoring_children_are_shown_and_the_rest_counted(self, tool):
        children = {f"AA{i}": _node(i / 10) for i in range(1, 9)}

        output = tool._build_output_node("AA", _node(0.0, children), "psc", {"AA": 8})

        assert [child["code"] for child in output["children"]] == ["AA8", "AA7", "AA6", "AA5", "AA4"]
        assert output["more_children"] == 3
        assert output["covers"] == 8

    def test_more_children_is_left_out_when_every_child_is_shown(self, tool):
        output = tool._build_output_node("AA", _node(0.0, {"AA1": _node(0.5), "AA2": _node(0.4)}), "psc", {})

        assert len(output["children"]) == 2
        assert "more_children" not in output

    def test_children_beneath_a_hidden_psc_group_count_toward_the_cap(self, tool):
        output = tool._build_output_node("X", _node(0.0, {"Service": _node(0.9, {"D": _node(0.9)})}), "psc", {})

        assert [child["code"] for child in output["children"]] == ["D"]


class TestBuildInstructions:
    @pytest.mark.parametrize(
        "code_type, inclusion, destination",
        [
            ("naics", "require", "naicsCodes.require"),
            ("naics", "exclude", "naicsCodes.exclude"),
            ("psc", "require", "pscCodes.require"),
            ("tas", "exclude", "tasCodes.exclude"),
            ("cfda", "require", "selectedCFDA"),
        ],
    )
    def test_names_where_the_chosen_codes_go(self, tool, code_type, inclusion, destination):
        assert destination in tool._build_instructions(code_type, inclusion, is_exact_code=False)

    def test_selection_guidance_weighs_description_score_and_position(self, tool):
        instructions = tool._build_instructions("naics", "require", is_exact_code=False)

        assert "best represent" in instructions
        assert "score" in instructions
        assert "leave this filter out" in instructions

    def test_selection_guidance_prefers_the_named_category_over_its_highest_scoring_part(self, tool):
        instructions = tool._build_instructions("psc", "require", is_exact_code=False)

        assert "even if codes beneath it score higher" in instructions
        assert "cover the same number of codes, choose the higher one" in instructions

    def test_exact_code_is_not_asked_to_be_weighed(self, tool):
        instructions = tool._build_instructions("naics", "require", is_exact_code=True)

        assert "itself a code" in instructions
        assert "best represent" not in instructions

    def test_only_tas_has_period_of_availability_guidance(self, tool):
        assert "period" in tool._build_instructions("tas", "require", is_exact_code=False)
        assert "period" not in tool._build_instructions("naics", "require", is_exact_code=False)


class TestLookupResultShape:
    async def test_result_carries_the_tree_and_instructions(self, tool, mock_embedding_generator, mock_expand_query):
        await _make_naics("5413", "Architectural services")
        await _make_naics("541330", "Engineering services")
        await _make_naics("541340", "Drafting services")

        result = await tool.lookup_codes("5413", "naics", inclusion="exclude")

        assert {key: result.results[key] for key in ("code_type", "inclusion", "query")} == {
            "code_type": "naics",
            "inclusion": "exclude",
            "query": "5413",
        }
        assert result.instructions and "naicsCodes.exclude" in result.instructions

    async def test_exact_code_query_returns_only_that_code_beneath_its_ancestors(
        self, tool, mock_embedding_generator, mock_expand_query
    ):
        await _make_naics("54", "Professional services")
        await _make_naics("5413", "Architectural services")
        await _make_naics("541330", "Engineering services")

        result = await tool.lookup_codes("5413", "naics")

        assert set(_tree_nodes(result)) == {"54", "5413"}
        assert _matched_codes(result) == {"5413"}

    async def test_naics_nodes_report_how_many_lowest_level_codes_they_cover(
        self, tool, mock_embedding_generator, mock_expand_query
    ):
        await _make_naics("212115", "Coal mining")
        await _make_naics("212114", "Surface coal mining")
        await _make_naics("2121", "Coal Mining")
        await _make_naics("212299", "Other mining")

        result = await tool.lookup_codes("212", "naics")

        nodes = _tree_nodes(result)
        assert (nodes["2121"]["covers"], nodes["212115"]["covers"]) == (2, 1)
        assert "212299" in nodes

    @pytest.mark.parametrize("code_type, query", [("naics", "541330"), ("psc", "B502"), ("cfda", "10.557")])
    async def test_filter_values_are_accepted_by_the_filters(
        self, tool, mock_embedding_generator, mock_expand_query, code_type, query
    ):
        """The llm copies each filter_value into execute_filter, so it has to validate there as it is."""
        await _make_naics("541330", "Engineering services")
        await _make_psc("B502", "Special studies")
        await _make_cfda("10.557", "Nutrition Program")

        result = await tool.lookup_codes(query, code_type)
        values = [_tree_nodes(result)[code]["filter_value"] for code in _matched_codes(result)]

        if code_type == "cfda":
            filters = await sync_to_async(Filters)(selectedCFDA={value["program_number"]: value for value in values})
            assert list(filters.selectedCFDA) == [query]
        else:
            filters = await sync_to_async(Filters)(**{f"{code_type}Codes": {"require": values}})
            assert getattr(filters, f"{code_type}Codes").require == values


class TestBuildFilterValue:
    def test_naics_is_a_flat_code(self, tool):
        assert tool._build_filter_value("541330", "naics") == "541330"

    @pytest.mark.parametrize(
        "code, expected",
        [
            ("B502", ["Service", "B", "B5", "B502"]),
            ("D3", ["Service", "D", "D3"]),
            ("D", ["Service", "D"]),
            ("1005", ["Product", "10", "1005"]),
            ("10", ["Product", "10"]),
            ("AA11", ["Research and Development", "AA", "AA1", "AA11"]),
            ("AA1", ["Research and Development", "AA", "AA1"]),
            # The client has no AA10, only the node AA1 that stands for it
            ("AA10", ["Research and Development", "AA", "AA1"]),
            ("AU10", ["Research and Development", "AU", "AU10"]),
            ("AA", ["Research and Development", "AA"]),
        ],
    )
    def test_psc_paths_have_a_segment_for_each_tier(self, tool, code, expected):
        assert tool._build_filter_value(code, "psc") == expected

    def test_assistance_listing_is_the_program_object(self, tool):
        assert tool._build_filter_value("14.257", "cfda", "Prevention") == {
            "identifier": "14.257",
            "program_number": "14.257",
            "program_title": "Prevention",
        }

    def test_psc_group_label_is_the_whole_path(self, tool):
        assert tool._build_filter_value("Research and Development", "psc") == ["Research and Development"]
        assert tool._build_filter_value("A", "psc") == ["Research and Development"]

    @pytest.mark.parametrize(
        "code, expected",
        [
            ("309-2025/2025-0200-000", ["309", "309-0200", "309-2025/2025-0200-000"]),
            ("309-X-0200-000", ["309", "309-0200", "309-X-0200-000"]),
            ("309-0200", ["309", "309-0200"]),
            ("309", ["309"]),
        ],
    )
    def test_tas_paths_start_at_the_agency(self, tool, code, expected):
        assert tool._build_filter_value(code, "tas") == expected


class TestSemanticScoreFloor:
    async def test_strong_match_is_returned(self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel):
        dims = getattr(NAICS, "embedding_dimensions", 256)
        await _make_naics("999999", "Unrelated description", embedding=_unit_vector(dims, 0))
        mock_embedding_generator.agenerate_embedding.return_value = _unit_vector(dims, 0)

        result = await tool.lookup_codes("query text", "naics")

        assert _matched_codes(result) == {"999999"}


class TestTasAccountQueries:
    @pytest.fixture(autouse=True)
    async def accounts(self):
        await _make_tas("014-2020/2021-0102-000", "Account A", agency_id="014", main="0102", bpoa="2020", epoa="2021")
        await _make_tas("014-2021/2022-0102-000", "Account A", agency_id="014", main="0102", bpoa="2021", epoa="2022")
        await _make_tas("310-X-0102-000", "Account B", agency_id="310", main="0102")
        await _make_tas("310-X-3200-000", "Account C", agency_id="310", main="3200")

    async def test_main_account_code_finds_the_account_in_every_agency(self, tool):
        result = await tool.lookup_codes("0102", "tas")

        nodes = _tree_nodes(result)
        assert {"014-0102", "310-0102"} <= set(nodes)
        assert "310-3200" not in nodes
        # An account code matches the accounts themselves, which are described by their titles
        assert [nodes[code]["matched"] for code in ("014-0102", "310-0102")] == [True, True]
        assert [nodes[code]["description"] for code in ("014-0102", "310-0102")] == ["Account A", "Account B"]
        # The accounts are the matches, so their periods are not offered as choices, though the accounts cover them
        assert not any("children" in nodes[code] for code in ("014-0102", "310-0102"))
        assert [nodes[code]["covers"] for code in ("014-0102", "310-0102")] == [2, 1]
        assert not any("-0102-000" in code for code in nodes)
        assert "every account in the tree matches it" in result.instructions

    async def test_aid_main_query_matches_the_account_not_each_year(self, tool):
        result = await tool.lookup_codes("014-0102", "tas")

        assert _matched_codes(result) == {"014-0102"}
        assert _tree_nodes(result)["014-0102"]["filter_value"] == ["014", "014-0102"]

    async def test_full_label_is_matched_at_the_label(self, tool):
        result = await tool.lookup_codes("014-2020/2021-0102-000", "tas")

        assert _matched_codes(result) == {"014-2020/2021-0102-000"}
        label = _tree_nodes(result)["014-2020/2021-0102-000"]
        assert label["filter_value"] == ["014", "014-0102", "014-2020/2021-0102-000"]

    async def test_exclude_inclusion_is_used_for_the_destination(self, tool):
        result = await tool.lookup_codes("014-0102", "tas", inclusion="exclude")

        assert result.results["inclusion"] == "exclude"
        assert "tasCodes.exclude" in result.instructions


class TestAIToolWiring:
    def test_input_schema_requires_query_and_code_type(self):
        schema = lookup_code_tool.description.input_schema
        assert set(schema["required"]) == {"query", "code_type"}

    def test_code_type_enum_matches_configs(self):
        schema = lookup_code_tool.description.input_schema
        enum_values = set(schema["properties"]["code_type"]["enum"])
        assert enum_values == set(CODE_TYPE_CONFIGS.keys())

    def test_logging_function_formats_message(self):
        msg = lookup_code_tool.logging({"code_type": "naics", "query": "construction"})
        assert "NAICS" in msg
        assert "construction" in msg

    async def test_tool_execution_through_function_attribute(self, mock_embedding_generator, mock_expand_query):
        await _make_naics("541330", "Construction of buildings")
        result = await lookup_code_tool.function(query="541330", code_type="naics")
        assert "541330" in _matched_codes(result)


class TestCodeTypeConfigsConsistency:
    @pytest.mark.parametrize("code_type", list(CODE_TYPE_CONFIGS.keys()))
    def test_config_has_required_fields(self, code_type):
        config = CODE_TYPE_CONFIGS[code_type]
        assert config.model_class is not None
        assert config.code_field
        assert config.description_field
        assert callable(config.get_all_ancestors)


class TestDedupeTasByPeriodOfAvailability:
    async def test_multiple_periods_same_account_collapse_to_best_score(
        self,
        tool,
        mock_embedding_generator,
        mock_expand_query,
        mock_get_aimodel,
    ):
        dims = getattr(TreasuryAppropriationAccount, "embedding_dimensions", 256)
        for year in ("2015", "2016", "2017"):
            await _make_tas(
                f"011-{year}/{year}-0001-000",
                "Test Tas",
                agency_id="011",
                main="0001",
                sub="000",
                bpoa=year,
                epoa=year,
                embedding=_unit_vector(dims, 0),
            )
        mock_embedding_generator.agenerate_embedding.return_value = _unit_vector(dims, 0)

        result = await tool.lookup_codes("test tas", "tas")

        # Only the best scoring period of the account is shown, not one for each year
        nodes = _tree_nodes(result)
        assert len(nodes["011-0001"]["children"]) == 1

    async def test_different_sub_account_not_collapsed(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel
    ):
        dims = getattr(TreasuryAppropriationAccount, "embedding_dimensions", 256)
        await _make_tas(
            "011-2016/2016-0001-000",
            "Main sub",
            agency_id="011",
            main="0001",
            sub="000",
            bpoa="2016",
            epoa="2016",
            embedding=_unit_vector(dims, 0),
        )
        await _make_tas(
            "011-2016/2016-0001-001",
            "Other sub",
            agency_id="011",
            main="0001",
            sub="001",
            bpoa="2016",
            epoa="2016",
            embedding=_unit_vector(dims, 0),
        )
        mock_embedding_generator.agenerate_embedding.return_value = _unit_vector(dims, 0)

        config = CODE_TYPE_CONFIGS["tas"]
        found = await tool._handle_hybrid_search("main sub", config, "tas", 20, None)
        assert tool._find_node(found["hierarchy"], "011-2016/2016-0001-000")
        assert tool._find_node(found["hierarchy"], "011-2016/2016-0001-001")

    async def test_different_ata_not_collapsed(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel
    ):
        dims = getattr(TreasuryAppropriationAccount, "embedding_dimensions", 256)
        await _make_tas(
            "011-2016/2016-0001-000",
            "No ATA",
            agency_id="011",
            main="0001",
            sub="000",
            ata=None,
            bpoa="2016",
            epoa="2016",
            embedding=_unit_vector(dims, 0),
        )
        await _make_tas(
            "[US_PHONE_REDACTED]/2016-0001-000",
            "With ATA",
            agency_id="011",
            main="0001",
            sub="000",
            ata="019",
            bpoa="2016",
            epoa="2016",
            embedding=_unit_vector(dims, 0),
        )
        mock_embedding_generator.agenerate_embedding.return_value = _unit_vector(dims, 0)

        config = CODE_TYPE_CONFIGS["tas"]
        found = await tool._handle_hybrid_search("compensation", config, "tas", 20, None)
        assert tool._find_node(found["hierarchy"], "011-2016/2016-0001-000")
        assert tool._find_node(found["hierarchy"], "[US_PHONE_REDACTED]/2016-0001-000")
