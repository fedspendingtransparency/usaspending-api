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


def _selected_codes(result, code_type: str, inclusion: str = "require") -> set[str]:
    """Every code in a lookup_codes result's require/exclude list, flattening PSC/TAS paths."""
    values = result.results[f"{code_type}Codes"][inclusion]
    return {code for value in values for code in ([value] if isinstance(value, str) else value)}


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

        assert "541330" in _selected_codes(result, "naics")
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

        codes_in_tree = _selected_codes(result, "naics")
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
        result = await tool._handle_exact_search("5413", config, "naics", {}, 20)
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

        assert "999999" in _selected_codes(result, "naics")
        mock_embedding_generator.agenerate_embedding.assert_called()

    async def test_vector_distance_threshold_excludes_far_matches(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel
    ):
        dims = getattr(NAICS, "embedding_dimensions", 256)

        await _make_naics("111111", "Close match", embedding=_unit_vector(dims, 0))
        await _make_naics("222222", "Far match", embedding=_unit_vector(dims, 1))  # orthogonal -> distance 1.0

        mock_embedding_generator.agenerate_embedding.return_value = _unit_vector(dims, 0)

        result = await tool.lookup_codes("query text", "naics")
        codes = _selected_codes(result, "naics")

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

        assert "333333" in _selected_codes(result, "naics")


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
        assert "error" in result.results

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
        assert "541330" in _selected_codes(result, "naics")


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

        codes = _selected_codes(result, "tas")
        tas_leaf_codes = {c for c in codes if c.count("-") == 3}
        assert len(tas_leaf_codes) == 1

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

        result = await tool.lookup_codes("main sub", "tas")

        codes = _selected_codes(result, "tas")
        assert "011-2016/2016-0001-000" in codes
        assert "011-2016/2016-0001-001" in codes

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

        result = await tool.lookup_codes("compensation", "tas")

        codes = _selected_codes(result, "tas")
        assert "011-2016/2016-0001-000" in codes
        assert "[US_PHONE_REDACTED]/2016-0001-000" in codes
