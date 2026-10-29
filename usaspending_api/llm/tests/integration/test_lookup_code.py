from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from asgiref.sync import sync_to_async
from model_bakery import baker

from usaspending_api.accounts.models.treasury_appropriation_account import TreasuryAppropriationAccount
from usaspending_api.llm.tools.exceptions import (
    AssistanceListingNotRecognizedError,
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


async def _make_cfda(program_number, program_title):
    return await sync_to_async(baker.make)(
        Cfda, program_number=program_number, program_title=program_title, _save_kwargs=SAVE_NO_EMBED
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
):
    tas = await sync_to_async(baker.make)(
        TreasuryAppropriationAccount,
        tas_rendering_label=label,
        account_title=title,
        agency_id=agency_id,
        main_account_code=main,
        sub_account_code=sub,
        allocation_transfer_agency_id=ata,
        beginning_period_of_availability=bpoa,
        ending_period_of_availability=epoa,
        _save_kwargs=SAVE_NO_EMBED,
    )
    if embedding is not None:
        tas.embedding = embedding
        await sync_to_async(tas.save)(**SAVE_NO_EMBED)
    return tas


async def _make_toptier_agency(toptier_code, name, abbreviation):
    return await sync_to_async(baker.make)(
        ToptierAgency,
        toptier_code=toptier_code,
        name=name,
        abbreviation=abbreviation,
        _save_kwargs=SAVE_NO_EMBED,
    )


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

        assert "541330" in result.results["naicsCodes"]["require"]
        mock_embedding_generator.agenerate_embedding.assert_not_called()
        mock_expand_query.assert_not_called()

    async def test_exact_match_score_is_one(self, tool, mock_embedding_generator, mock_expand_query):
        await _make_naics("541330", "Construction of buildings")

        config = CODE_TYPE_CONFIGS["naics"]
        result = await tool._handle_exact_search("541330", config, "naics", {}, 20)
        node = result["hierarchy"]["541330"]
        assert node["score"] == 1.0

    async def test_exact_match_case_insensitive_input(self, tool, mock_embedding_generator, mock_expand_query):
        await _make_cfda("10.557", "Nutrition Program")

        result = await tool.lookup_codes("10.557", "cfda")
        assert "10.557" in result.results["selectedCFDA"]


class TestPrefixMatch:
    async def test_prefix_match_returns_all_matching_codes(self, tool, mock_embedding_generator, mock_expand_query):
        await _make_naics("541330", "Construction of buildings")
        await _make_naics("541340", "Drafting services")
        await _make_naics("541511", "Custom computer programming")

        result = await tool.lookup_codes("5413", "naics")

        codes_in_tree = result.results["naicsCodes"]["require"]
        assert "541330" in codes_in_tree
        assert "541340" in codes_in_tree
        assert "541511" not in codes_in_tree

    async def test_prefix_match_skips_embeddings(self, tool, mock_embedding_generator, mock_expand_query):
        await _make_naics("541330", "Construction of buildings")

        await tool.lookup_codes("5413", "naics")

        mock_embedding_generator.agenerate_embedding.assert_not_called()
        mock_expand_query.assert_not_called()


class TestHybridSearch:
    async def test_falls_through_to_embedding_search(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel
    ):
        dims = getattr(NAICS, "embedding_dimensions", 256)
        await _make_naics("999999", "Totally unrelated description", embedding=_unit_vector(dims, 0))

        mock_embedding_generator.agenerate_embedding.return_value = _unit_vector(dims, 0)

        result = await tool.lookup_codes("some semantic query", "naics")

        assert "999999" in result.results["naicsCodes"]["require"]
        mock_embedding_generator.agenerate_embedding.assert_called()

    async def test_vector_distance_threshold_excludes_far_matches(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel
    ):
        dims = getattr(NAICS, "embedding_dimensions", 256)

        await _make_naics("111111", "Close match", embedding=_unit_vector(dims, 0))
        await _make_naics("222222", "Far match", embedding=_unit_vector(dims, 1))  # orthogonal -> distance 1.0

        mock_embedding_generator.agenerate_embedding.return_value = _unit_vector(dims, 0)

        result = await tool.lookup_codes("query text", "naics")
        codes = result.results["naicsCodes"]["require"]

        assert "111111" in codes
        assert "222222" not in codes

    async def test_top_k_truncates_results(self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel):
        dims = getattr(NAICS, "embedding_dimensions", 256)
        mock_embedding_generator.agenerate_embedding.return_value = _unit_vector(dims, 0)

        for i in range(5):
            await _make_naics(f"10000{i}", f"Match {i}", embedding=_unit_vector(dims, 0))

        result = await tool.lookup_codes("query text", "naics", top_k=2)
        assert len(result.results["naicsCodes"]["require"]) <= 2

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

        assert "333333" in result.results["naicsCodes"]["require"]


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

        result = await tool._finalize_results(all_results, config, "naics", {}, top_k=20)

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

        result = await tool._finalize_results(all_results, config, "naics", {}, top_k=20)

        roots = list(result["hierarchy"].keys())
        assert roots[0] == "B1"  # best descendant score (0.9) wins over A1 (0.2)
        assert roots[1] == "A1"

    async def test_top_k_applied_before_ancestor_lookup(self, tool, monkeypatch):
        config = CODE_TYPE_CONFIGS["naics"]
        monkeypatch.setattr(config, "get_parent_code", lambda code: None)
        monkeypatch.setattr(config, "get_all_ancestors", lambda code: [])

        all_results = {f"C{i}": CodeResult(code=f"C{i}", description=f"desc {i}", score=float(i)) for i in range(5)}

        result = await tool._finalize_results(all_results, config, "naics", {}, top_k=2)

        assert result["total_codes"] == 2
        assert set(result["hierarchy"].keys()) == {"C4", "C3"}

    async def test_empty_results_returns_empty_hierarchy(self, tool):
        config = CODE_TYPE_CONFIGS["naics"]
        result = await tool._finalize_results({}, config, "naics", {}, top_k=20)
        assert result == {"hierarchy": {}, "total_codes": 0}

    async def test_non_psc_code_type_does_not_apply_tier1_grouping(self, tool, monkeypatch):
        """The synthetic Tier1 grouping step is PSC-only; other code types keep their real root
        codes as top-level hierarchy keys."""
        config = CODE_TYPE_CONFIGS["naics"]
        monkeypatch.setattr(config, "get_parent_code", lambda code: None)
        monkeypatch.setattr(config, "get_all_ancestors", lambda code: [])

        all_results = {"33": CodeResult(code="33", description="Manufacturing", score=0.5)}
        result = await tool._finalize_results(all_results, config, "naics", {}, top_k=20)

        assert set(result["hierarchy"].keys()) == {"33"}


class TestGroupPscRootsByTier1:
    """Tests for CodeLookupTool._group_psc_roots_by_tier1, the PSC-only post-processing step that
    wraps each real PSC root ('A', 'D', '10', ...) under its synthetic Tier1 PSC_GROUPS label
    (Research and Development / Service / Product), matching the grouping the real PSC filter tree
    API uses (PSCFilterTree.toptier_search).
    """

    @staticmethod
    def _root_node(code, score=0.5, children=None, leaf_node_count=1):
        return {
            "code": code,
            "description": f"description for {code}",
            "score": score,
            "is_root": True,
            "is_leaf": not children,
            "children": children or {},
            "leaf_node_count": leaf_node_count,
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

    def test_service_and_product_roots_are_nested_as_children_and_marked_non_root(self):
        hierarchy = {"D": self._root_node("D"), "10": self._root_node("10")}

        grouped = CodeLookupTool._group_psc_roots_by_tier1(hierarchy)

        assert grouped["Service"]["children"]["D"]["is_root"] is False
        assert grouped["Product"]["children"]["10"]["is_root"] is False

    def test_leaf_node_count_summed_across_roots_in_same_group(self):
        hierarchy = {
            "D": self._root_node("D", leaf_node_count=3),
            "F": self._root_node("F", leaf_node_count=4),
        }

        grouped = CodeLookupTool._group_psc_roots_by_tier1(hierarchy)

        assert grouped["Service"]["leaf_node_count"] == 7

    def test_leaf_node_count_not_summed_across_different_groups(self):
        hierarchy = {
            "D": self._root_node("D", leaf_node_count=3),
            "10": self._root_node("10", leaf_node_count=4),
        }

        grouped = CodeLookupTool._group_psc_roots_by_tier1(hierarchy)

        assert grouped["Service"]["leaf_node_count"] == 3
        assert grouped["Product"]["leaf_node_count"] == 4

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

        assert set(d.keys()) == {"code", "description", "score", "children", "is_root", "is_leaf"}
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


class TestAddTasAncestors:
    async def test_toptier_ancestor_from_agency_table(self, tool):
        await _make_toptier_agency("012", "Test Agency", "TA")

        all_results: dict[str, CodeResult] = {}
        await tool._add_tas_ancestors({"012"}, all_results, budget_bureau_names={})

        assert "012" in all_results
        assert all_results["012"].description == "Test Agency (TA)"

    async def test_aid_main_ancestor_from_budget_bureau_names(self, tool):
        all_results: dict[str, CodeResult] = {}
        await tool._add_tas_ancestors({"012-0123"}, all_results, budget_bureau_names={"012-0123": "Test Bureau"})

        assert all_results["012-0123"].description == "Test Bureau"

    async def test_does_not_overwrite_existing_entry(self, tool):
        await _make_toptier_agency("012", "Test Agency", "TA")
        existing = CodeResult(code="012", description="Already present", score=1.0)
        all_results = {"012": existing}

        await tool._add_tas_ancestors({"012"}, all_results, budget_bureau_names={})

        assert all_results["012"] is existing

    async def test_missing_ancestor_data_is_silently_skipped(self, tool):
        all_results: dict[str, CodeResult] = {}
        await tool._add_tas_ancestors({"999"}, all_results, budget_bureau_names={})
        assert all_results == {}


class TestErrorHandling:
    async def test_unsupported_code_type_returns_error(self, tool):
        result = await tool.lookup_codes("anything", "bogus")
        assert (
            result.results.get("error")
            == "Unsupported code type: bogus. Supported types: ['naics', 'psc', 'cfda', 'tas']"
        )

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

        assert list(result.results["selectedCFDA"]) == ["14.257"]
        mock_embedding_generator.agenerate_embedding.assert_not_called()


class TestSelectMatchedNodes:
    def test_ancestors_with_no_score_are_not_selected(self, tool):
        hierarchy = {"33": _node(0.0, {"3315": _node(0.0, {"331511": _node(0.95)})})}

        assert [code for code, _ in tool._select_matched_nodes(hierarchy, "naics")] == ["331511"]

    def test_matched_code_covers_its_lower_scoring_descendants(self, tool):
        hierarchy = {"23": _node(1.0, {"2362": _node(0.95, {"236220": _node(0.95)})})}

        assert [code for code, _ in tool._select_matched_nodes(hierarchy, "naics")] == ["23"]

    def test_higher_scoring_descendant_is_selected_over_its_ancestor(self, tool):
        hierarchy = {"21": _node(0.6, {"2121": _node(0.0, {"212115": _node(0.9)})})}

        assert [code for code, _ in tool._select_matched_nodes(hierarchy, "naics")] == ["212115"]

    def test_scores_below_the_minimum_are_not_selected(self, tool):
        hierarchy = {"23": _node(0.9), "33": _node(0.4, {"3366": _node(0.3)}), "54": _node(0.8)}

        selected = [code for code, _ in tool._select_matched_nodes(hierarchy, "naics", min_score=0.72)]

        assert selected == ["23", "54"]

    def test_descendant_must_beat_its_ancestor_by_the_margin(self, tool):
        hierarchy = {"21": _node(0.6, {"212115": _node(0.63)})}

        assert [code for code, _ in tool._select_matched_nodes(hierarchy, "naics", margin=0.05)] == ["21"]

    def test_descendant_that_clearly_outscores_its_ancestor_is_selected(self, tool):
        hierarchy = {"21": _node(0.6, {"212115": _node(0.7)})}

        assert [code for code, _ in tool._select_matched_nodes(hierarchy, "naics", margin=0.05)] == ["212115"]

    def test_psc_group_matched_itself_is_selected_over_its_lower_scoring_children(self, tool):
        hierarchy = {
            "Research and Development": {**_node(0.98, {"AC": _node(0.2)}), "is_group_match": True},
        }

        selected = tool._select_matched_nodes(hierarchy, "psc", min_score=0.78)

        assert [code for code, _ in selected] == ["Research and Development"]

    def test_psc_group_labels_are_never_selected(self, tool):
        hierarchy = {"Service": _node(0.9, {"D": _node(0.0, {"D3": _node(0.9)})})}

        assert [code for code, _ in tool._select_matched_nodes(hierarchy, "psc")] == ["D3"]


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
            ("AU10", ["Research and Development", "AU", "AU10"]),
            ("AA", ["Research and Development", "AA"]),
        ],
    )
    def test_psc_paths_have_a_segment_for_each_tier(self, tool, code, expected):
        assert tool._build_filter_value(code, "psc") == expected

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

        assert result.results["naicsCodes"]["require"] == ["999999"]


class TestTasAccountQueries:
    @pytest.fixture(autouse=True)
    async def accounts(self):
        await _make_tas("014-2020/2021-0102-000", "Account A", agency_id="014", main="0102", bpoa="2020", epoa="2021")
        await _make_tas("014-2021/2022-0102-000", "Account A", agency_id="014", main="0102", bpoa="2021", epoa="2022")
        await _make_tas("310-X-0102-000", "Account B", agency_id="310", main="0102")
        await _make_tas("310-X-3200-000", "Account C", agency_id="310", main="3200")

    async def test_main_account_code_finds_the_account_in_every_agency(self, tool):
        result = await tool.lookup_codes("0102", "tas")

        assert sorted(result.results["tasCodes"]["require"]) == [["014", "014-0102"], ["310", "310-0102"]]

    async def test_aid_main_query_selects_the_account_not_each_year(self, tool):
        result = await tool.lookup_codes("014-0102", "tas")

        assert result.results["tasCodes"]["require"] == [["014", "014-0102"]]

    async def test_full_label_stays_at_the_label(self, tool):
        result = await tool.lookup_codes("014-2020/2021-0102-000", "tas")

        assert result.results["tasCodes"]["require"] == [["014", "014-0102", "014-2020/2021-0102-000"]]

    async def test_exclude_inclusion_is_used_for_the_key(self, tool):
        result = await tool.lookup_codes("014-0102", "tas", inclusion="exclude")

        assert result.results["tasCodes"]["exclude"] == [["014", "014-0102"]]
        assert result.results["tasCodes"]["require"] == []


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
        assert "541330" in result.results["naicsCodes"]["require"]


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

        assert result.results["tasCodes"]["require"] == [["011", "011-0001"]]

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
        found = await tool._handle_hybrid_search("main sub", config, "tas", {}, 20, None)

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
        found = await tool._handle_hybrid_search("compensation", config, "tas", {}, 20, None)

        assert tool._find_node(found["hierarchy"], "011-2016/2016-0001-000")
        assert tool._find_node(found["hierarchy"], "[US_PHONE_REDACTED]/2016-0001-000")
