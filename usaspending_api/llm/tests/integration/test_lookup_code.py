from unittest.mock import MagicMock, patch

import pytest
from model_bakery import baker

from usaspending_api.accounts.models.treasury_appropriation_account import TreasuryAppropriationAccount
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

pytestmark = pytest.mark.django_db


SAVE_NO_EMBED = {"auto_generate_embedding": False}


def _unit_vector(dimensions: int, index: int) -> list[float]:
    """Build a unit vector with a 1.0 at `index` so orthogonal vectors have
    cosine distance 1.0 (excluded by the 0.75 threshold) and identical
    vectors have cosine distance 0.0 (included)."""
    vec = [0.0] * dimensions
    vec[index] = 1.0
    return vec


def _make_naics(code, description, embedding=None):
    naics = baker.make(NAICS, code=code, description=description, _save_kwargs=SAVE_NO_EMBED)
    if embedding is not None:
        naics.embedding = embedding
        naics.save(**SAVE_NO_EMBED)
    return naics


def _make_psc(code, description):
    return baker.make(PSC, code=code, description=description, length=len(code), _save_kwargs=SAVE_NO_EMBED)


def _make_cfda(program_number, program_title):
    return baker.make(Cfda, program_number=program_number, program_title=program_title, _save_kwargs=SAVE_NO_EMBED)


def _make_tas(
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
        _save_kwargs=SAVE_NO_EMBED,
    )
    if embedding is not None:
        tas.embedding = embedding
        tas.save(**SAVE_NO_EMBED)
    return tas


def _make_toptier_agency(toptier_code, name, abbreviation):
    return baker.make(
        ToptierAgency,
        toptier_code=toptier_code,
        name=name,
        abbreviation=abbreviation,
        _save_kwargs=SAVE_NO_EMBED,
    )


@pytest.fixture
def mock_embedding_generator():
    """Mock EmbeddingGenerator so no real embedding API calls are made.
    Returns the mock instance so tests can control generate_embedding output."""
    with patch("usaspending_api.llm.tools.lookup_code.EmbeddingGenerator") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_expand_query():
    """By default, disable fanout expansion so hybrid search runs against
    only the original query string."""
    with patch("usaspending_api.llm.tools.lookup_code.expand_query") as mock_fn:
        mock_fn.side_effect = lambda query, model, query_fanout: [query]
        yield mock_fn


@pytest.fixture
def mock_get_aimodel():
    with patch("usaspending_api.llm.tools.lookup_code.AIModel.objects.get") as mock_fn:
        mock_fn.side_effect = None
        yield mock_fn


@pytest.fixture
def tool():
    return CodeLookupTool()


def _flatten_codes(hierarchy: dict) -> set[str]:
    codes = set()
    for code, node in hierarchy.items():
        codes.add(code)
        codes.update(_flatten_codes(node.get("children", {})))
    return codes


class TestQueryExactAndPrefixMatches:
    def test_exact_match_case_insensitive(self, tool):
        _make_naics("541330", "Test construction")
        config = CODE_TYPE_CONFIGS["naics"]

        exact_qs, prefix_qs = tool._query_exact_and_prefix_matches(config, "541330")
        assert list(exact_qs.values_list("code", flat=True)) == ["541330"]

    def test_prefix_match_excludes_exact(self, tool):
        _make_naics("5413", "Parent")
        _make_naics("541330", "Child")
        config = CODE_TYPE_CONFIGS["naics"]

        exact_qs, prefix_qs = tool._query_exact_and_prefix_matches(config, "5413")
        assert list(exact_qs.values_list("code", flat=True)) == ["5413"]
        assert list(prefix_qs.values_list("code", flat=True)) == ["541330"]

    def test_no_matches_returns_empty_querysets(self, tool):
        config = CODE_TYPE_CONFIGS["naics"]
        exact_qs, prefix_qs = tool._query_exact_and_prefix_matches(config, "999999")
        assert not exact_qs.exists()
        assert not prefix_qs.exists()


class TestExactMatchShortCircuit:
    def test_exact_match_skips_embeddings_and_fanout(self, tool, mock_embedding_generator, mock_expand_query):
        _make_naics("541330", "Construction of buildings")

        result = tool.lookup_codes("541330", "naics", query_fanout=3)

        assert "541330" in _flatten_codes(result["hierarchy"])
        mock_embedding_generator.generate_embedding.assert_not_called()
        mock_expand_query.assert_not_called()

    def test_exact_match_score_is_one(self, tool, mock_embedding_generator, mock_expand_query):
        _make_naics("541330", "Construction of buildings")

        result = tool.lookup_codes("541330", "naics")
        node = result["hierarchy"]["541330"]
        assert node["score"] == 1.0

    def test_exact_match_case_insensitive_input(self, tool, mock_embedding_generator, mock_expand_query):
        _make_cfda("10.557", "Nutrition Program")

        result = tool.lookup_codes("10.557", "cfda")
        assert "10.557" in result["hierarchy"]


class TestPrefixMatch:
    def test_prefix_match_returns_all_matching_codes(self, tool, mock_embedding_generator, mock_expand_query):
        _make_naics("541330", "Construction of buildings")
        _make_naics("541340", "Drafting services")
        _make_naics("541511", "Custom computer programming")

        result = tool.lookup_codes("5413", "naics")

        codes_in_tree = _flatten_codes(result["hierarchy"])
        assert "541330" in codes_in_tree
        assert "541340" in codes_in_tree
        assert "541511" not in codes_in_tree

    def test_prefix_match_skips_embeddings(self, tool, mock_embedding_generator, mock_expand_query):
        _make_naics("541330", "Construction of buildings")

        tool.lookup_codes("5413", "naics")

        mock_embedding_generator.generate_embedding.assert_not_called()
        mock_expand_query.assert_not_called()

    def test_prefix_match_score_below_exact(self, tool, mock_embedding_generator, mock_expand_query):
        _make_naics("541330", "Construction of buildings")

        result = tool.lookup_codes("5413", "naics")
        node = result["hierarchy"]["541330"]
        assert node["score"] == 0.95


class TestHybridSearch:
    def test_falls_through_to_embedding_search(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel
    ):
        dims = getattr(NAICS, "embedding_dimensions", 256)
        _make_naics("999999", "Totally unrelated description", embedding=_unit_vector(dims, 0))

        mock_embedding_generator.generate_embedding.return_value = _unit_vector(dims, 0)

        result = tool.lookup_codes("some semantic query", "naics")

        assert "999999" in _flatten_codes(result["hierarchy"])
        mock_embedding_generator.generate_embedding.assert_called()

    def test_vector_distance_threshold_excludes_far_matches(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel
    ):
        dims = getattr(NAICS, "embedding_dimensions", 256)

        _make_naics("111111", "Close match", embedding=_unit_vector(dims, 0))
        _make_naics("222222", "Far match", embedding=_unit_vector(dims, 1))  # orthogonal -> distance 1.0

        mock_embedding_generator.generate_embedding.return_value = _unit_vector(dims, 0)

        result = tool.lookup_codes("query text", "naics")
        codes = _flatten_codes(result["hierarchy"])

        assert "111111" in codes
        assert "222222" not in codes

    def test_top_k_truncates_results(self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel):
        dims = getattr(NAICS, "embedding_dimensions", 256)
        mock_embedding_generator.generate_embedding.return_value = _unit_vector(dims, 0)

        for i in range(5):
            _make_naics(f"10000{i}", f"Match {i}", embedding=_unit_vector(dims, 0))

        result = tool.lookup_codes("query text", "naics", top_k=2)
        assert result["total_codes"] <= 2

    def test_use_fanout_false_does_not_call_expand_query(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel
    ):
        dims = getattr(NAICS, "embedding_dimensions", 256)
        mock_embedding_generator.generate_embedding.return_value = _unit_vector(dims, 0)

        tool.lookup_codes("query text", "naics", query_fanout=None)

        mock_expand_query.assert_not_called()

    def test_embedding_failure_does_not_raise(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel
    ):
        mock_embedding_generator.generate_embedding.side_effect = Exception("API down")

        result = tool.lookup_codes("query text", "naics")

        assert result["hierarchy"] == {}
        assert result["total_codes"] == 0

    def test_duplicate_matches_across_fanout_keep_higher_score(self, tool, mock_embedding_generator, mock_get_aimodel):
        dims = getattr(NAICS, "embedding_dimensions", 256)
        _make_naics("333333", "Some match", embedding=_unit_vector(dims, 0))

        with patch("usaspending_api.llm.tools.lookup_code.expand_query") as mock_expand:
            mock_expand.return_value = ["variation a", "variation b"]
            mock_embedding_generator.generate_embedding.return_value = _unit_vector(dims, 0)

            result = tool.lookup_codes("query text", "naics", query_fanout=3)

        assert "333333" in _flatten_codes(result["hierarchy"])


class TestFinalizeResultsHierarchy:
    def test_ancestors_are_pulled_in_and_nested(self, tool, monkeypatch):
        _make_naics("54", "Professional services")
        _make_naics("5413", "Architectural/engineering")
        _make_naics("541330", "Engineering services")

        parent_map = {"541330": "5413", "5413": "54"}
        ancestor_map = {"541330": ["5413", "54"], "5413": ["54"], "54": []}

        config = CODE_TYPE_CONFIGS["naics"]
        monkeypatch.setattr(config, "get_parent_code", lambda code: parent_map.get(code))
        monkeypatch.setattr(config, "get_all_ancestors", lambda code: ancestor_map.get(code, []))

        all_results = {"541330": CodeResult(code="541330", description="Engineering services", score=1.0)}

        result = tool._finalize_results(all_results, config, "naics", {}, top_k=20)

        assert "54" in result["hierarchy"]
        assert "5413" in result["hierarchy"]["54"]["children"]
        assert "541330" in result["hierarchy"]["54"]["children"]["5413"]["children"]
        assert result["total_codes"] == 3

    def test_multiple_roots_sorted_by_best_score_in_tree(self, tool, monkeypatch):
        config = CODE_TYPE_CONFIGS["naics"]
        monkeypatch.setattr(config, "get_parent_code", lambda code: {"B2": "B1"}.get(code))
        monkeypatch.setattr(config, "get_all_ancestors", lambda code: [])

        all_results = {
            "A1": CodeResult(code="A1", description="Low root", score=0.2),
            "B1": CodeResult(code="B1", description="High root parent", score=0.1),
            "B2": CodeResult(code="B2", description="High score child", score=0.9),
        }

        result = tool._finalize_results(all_results, config, "naics", {}, top_k=20)

        roots = list(result["hierarchy"].keys())
        assert roots[0] == "B1"  # best descendant score (0.9) wins over A1 (0.2)
        assert roots[1] == "A1"

    def test_top_k_applied_before_ancestor_lookup(self, tool, monkeypatch):
        config = CODE_TYPE_CONFIGS["naics"]
        monkeypatch.setattr(config, "get_parent_code", lambda code: None)
        monkeypatch.setattr(config, "get_all_ancestors", lambda code: [])

        all_results = {f"C{i}": CodeResult(code=f"C{i}", description=f"desc {i}", score=float(i)) for i in range(5)}

        result = tool._finalize_results(all_results, config, "naics", {}, top_k=2)

        assert result["total_codes"] == 2
        assert set(result["hierarchy"].keys()) == {"C4", "C3"}

    def test_empty_results_returns_empty_hierarchy(self, tool):
        config = CODE_TYPE_CONFIGS["naics"]
        result = tool._finalize_results({}, config, "naics", {}, top_k=20)
        assert result == {"hierarchy": {}, "total_codes": 0}


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


class TestAddTasAncestors:
    def test_toptier_ancestor_from_agency_table(self, tool):
        _make_toptier_agency("012", "Test Agency", "TA")

        all_results: dict[str, CodeResult] = {}
        tool._add_tas_ancestors({"012"}, all_results, budget_bureau_names={})

        assert "012" in all_results
        assert all_results["012"].description == "Test Agency (TA)"

    def test_aid_main_ancestor_from_budget_bureau_names(self, tool):
        all_results: dict[str, CodeResult] = {}
        tool._add_tas_ancestors({"012-0123"}, all_results, budget_bureau_names={"012-0123": "Test Bureau"})

        assert all_results["012-0123"].description == "Test Bureau"

    def test_does_not_overwrite_existing_entry(self, tool):
        _make_toptier_agency("012", "Test Agency", "TA")
        existing = CodeResult(code="012", description="Already present", score=1.0)
        all_results = {"012": existing}

        tool._add_tas_ancestors({"012"}, all_results, budget_bureau_names={})

        assert all_results["012"] is existing

    def test_missing_ancestor_data_is_silently_skipped(self, tool):
        all_results: dict[str, CodeResult] = {}
        tool._add_tas_ancestors({"999"}, all_results, budget_bureau_names={})
        assert all_results == {}


class TestErrorHandling:
    def test_unsupported_code_type_returns_error(self, tool):
        result = tool.lookup_codes("anything", "bogus")
        assert "error" in result
        assert result["results"] == []

    def test_no_matches_anywhere_returns_empty_hierarchy(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel
    ):
        mock_embedding_generator.generate_embedding.return_value = None

        result = tool.lookup_codes("nothing matches this", "naics")
        assert result == {"hierarchy": {}, "total_codes": 0}


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

    def test_tool_execution_through_function_attribute(self, mock_embedding_generator, mock_expand_query):
        _make_naics("541330", "Construction of buildings")
        result = lookup_code_tool.function(query="541330", code_type="naics")
        assert "541330" in _flatten_codes(result["hierarchy"])


class TestCodeTypeConfigsConsistency:
    @pytest.mark.parametrize("code_type", list(CODE_TYPE_CONFIGS.keys()))
    def test_config_has_required_fields(self, code_type):
        config = CODE_TYPE_CONFIGS[code_type]
        assert config.model_class is not None
        assert config.code_field
        assert config.description_field
        assert callable(config.get_all_ancestors)


class TestDedupeTasByPeriodOfAvailability:
    def test_multiple_periods_same_account_collapse_to_best_score(
        self,
        tool,
        mock_embedding_generator,
        mock_expand_query,
        mock_get_aimodel,
    ):
        dims = getattr(TreasuryAppropriationAccount, "embedding_dimensions", 256)
        for year in ("2015", "2016", "2017"):
            _make_tas(
                f"011-{year}/{year}-0001-000",
                "Test Tas",
                agency_id="011",
                main="0001",
                sub="000",
                bpoa=year,
                epoa=year,
                embedding=_unit_vector(dims, 0),
            )
        mock_embedding_generator.generate_embedding.return_value = _unit_vector(dims, 0)

        result = tool.lookup_codes("test tas", "tas")

        codes = _flatten_codes(result["hierarchy"])
        tas_leaf_codes = {c for c in codes if c.count("-") == 3}
        assert len(tas_leaf_codes) == 1

    def test_different_sub_account_not_collapsed(
        self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel
    ):
        dims = getattr(TreasuryAppropriationAccount, "embedding_dimensions", 256)
        _make_tas(
            "011-2016/2016-0001-000",
            "Main sub",
            agency_id="011",
            main="0001",
            sub="000",
            bpoa="2016",
            epoa="2016",
            embedding=_unit_vector(dims, 0),
        )
        _make_tas(
            "011-2016/2016-0001-001",
            "Other sub",
            agency_id="011",
            main="0001",
            sub="001",
            bpoa="2016",
            epoa="2016",
            embedding=_unit_vector(dims, 0),
        )
        mock_embedding_generator.generate_embedding.return_value = _unit_vector(dims, 0)

        result = tool.lookup_codes("main sub", "tas")

        codes = _flatten_codes(result["hierarchy"])
        assert "011-2016/2016-0001-000" in codes
        assert "011-2016/2016-0001-001" in codes

    def test_different_ata_not_collapsed(self, tool, mock_embedding_generator, mock_expand_query, mock_get_aimodel):
        dims = getattr(TreasuryAppropriationAccount, "embedding_dimensions", 256)
        _make_tas(
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
        _make_tas(
            "019-011-2016/2016-0001-000",
            "With ATA",
            agency_id="011",
            main="0001",
            sub="000",
            ata="019",
            bpoa="2016",
            epoa="2016",
            embedding=_unit_vector(dims, 0),
        )
        mock_embedding_generator.generate_embedding.return_value = _unit_vector(dims, 0)

        result = tool.lookup_codes("compensation", "tas")

        codes = _flatten_codes(result["hierarchy"])
        assert "011-2016/2016-0001-000" in codes
        assert "019-011-2016/2016-0001-000" in codes
