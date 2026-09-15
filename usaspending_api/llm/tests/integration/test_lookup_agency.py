import itertools
import uuid
from unittest.mock import MagicMock, patch

import pytest
from model_bakery import baker

from usaspending_api.llm.tools.lookup_agency import AgencyLookupTool, lookup_agency_tool
from usaspending_api.references.models.agency import Agency
from usaspending_api.references.models.subtier_agency import SubtierAgency
from usaspending_api.references.models.toptier_agency import ToptierAgency
from usaspending_api.search.models.award_search import AwardSearch
from usaspending_api.search.models.mv_agency_autocomplete import AgencyAutocompleteMatview

pytestmark = pytest.mark.django_db


SAVE_NO_EMBED = {"auto_generate_embedding": False}


def _unit_vector(dimensions: int, index: int) -> list[float]:
    """Build a unit vector with a 1.0 at `index` so orthogonal vectors have
    cosine distance 1.0 (excluded by the 0.75 threshold) and identical
    vectors have cosine distance 0.0 (included)."""
    vec = [0.0] * dimensions
    vec[index] = 1.0
    return vec


def _make_toptier_agency(toptier_code, name, abbreviation, embedding=None):
    toptier = baker.make(
        ToptierAgency,
        toptier_code=toptier_code,
        name=name,
        abbreviation=abbreviation,
        _save_kwargs=SAVE_NO_EMBED,
    )
    if embedding is not None:
        toptier.embedding = embedding
        toptier.save(**SAVE_NO_EMBED)
    return toptier


def _make_subtier_agency(name, abbreviation, embedding=None):
    subtier = baker.make(
        SubtierAgency,
        name=name,
        abbreviation=abbreviation,
        _save_kwargs=SAVE_NO_EMBED,
    )
    if embedding is not None:
        subtier.embedding = embedding
        subtier.save(**SAVE_NO_EMBED)
    return subtier


def _make_agency(toptier_agency, subtier_agency=None, toptier_flag=True):
    return baker.make(
        Agency,
        toptier_agency=toptier_agency,
        subtier_agency=subtier_agency,
        toptier_flag=toptier_flag,
    )


_award_id_counter = itertools.count(1)


def _make_award_search(agency, cited_as="awarding", certified_date="2020-01-01"):
    kwargs = {
        "award_id": next(_award_id_counter),
        "certified_date": certified_date,
        "generated_unique_award_id": f"TEST_AWARD_{uuid.uuid4()}",
    }
    if cited_as == "awarding":
        kwargs["awarding_agency_id"] = agency.id
    else:
        kwargs["funding_agency_id"] = agency.id
    return baker.make(AwardSearch, **kwargs)


_subtier_counter = itertools.count(1)


def _make_matview_row(
    toptier_code,
    toptier_name,
    toptier_abbreviation=None,
    subtier_name=None,
    subtier_abbreviation=None,
    toptier_flag=True,
    cited_as="awarding",
):
    """Create real Agency/ToptierAgency/SubtierAgency rows plus a citing
    AwardSearch row, then fetch the resulting row from the traditional view
    (mv_agency_autocomplete is a plain view in tests, so it reflects
    underlying table data immediately with no refresh needed)."""
    toptier = _make_toptier_agency(toptier_code, toptier_name, toptier_abbreviation)

    if subtier_name is None:
        n = next(_subtier_counter)
        subtier_name = f"Placeholder Subtier {n}"
        subtier_abbreviation = subtier_abbreviation or f"PS{n}"
    subtier = _make_subtier_agency(subtier_name, subtier_abbreviation)

    agency = _make_agency(toptier, subtier_agency=subtier, toptier_flag=toptier_flag)
    _make_award_search(agency, cited_as=cited_as)

    return AgencyAutocompleteMatview.objects.get(
        toptier_code=toptier_code, toptier_name=toptier_name, subtier_name=subtier_name
    )


@pytest.fixture
def mock_embedding_generator():
    with patch("usaspending_api.llm.tools.lookup_agency.EmbeddingGenerator") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def tool():
    return AgencyLookupTool()


class TestQueryExactAndPrefixMatches:
    def test_exact_match_case_insensitive_on_toptier_code(self, tool):
        _make_matview_row("080", "National Aeronautics and Space Administration", "NASA")

        matches = tool._query_exact_and_prefix_matches("080")

        assert list(matches.values_list("toptier_code", flat=True)) == ["080"]

    def test_exact_match_case_insensitive_on_name(self, tool):
        _make_matview_row("080", "National Aeronautics and Space Administration", "NASA")

        matches = tool._query_exact_and_prefix_matches("nasa")

        assert list(matches.values_list("toptier_code", flat=True)) == ["080"]

    def test_exact_match_takes_priority_over_prefix(self, tool):
        _make_matview_row("100", "Nat", "NAT")
        _make_matview_row("200", "National Archives", "NARA")

        matches = tool._query_exact_and_prefix_matches("Nat")

        codes = list(matches.values_list("toptier_code", flat=True))
        assert codes == ["100"]

    def test_prefix_match_used_when_no_exact_match(self, tool):
        _make_matview_row("200", "National Archives and Records Administration", "NARA")
        _make_matview_row("300", "National Credit Union Administration", "NCUA")
        _make_matview_row("400", "Department of Energy", "DOE")

        matches = tool._query_exact_and_prefix_matches("Nat")

        codes = set(matches.values_list("toptier_code", flat=True))
        assert codes == {"200", "300"}

    def test_no_matches_returns_empty_queryset(self, tool):
        matches = tool._query_exact_and_prefix_matches("zzz-not-a-real-agency")
        assert not matches.exists()


class TestLookupAgenciesExactAndPrefix:
    def test_exact_match_skips_embedding_generation(self, tool, mock_embedding_generator):
        _make_matview_row("080", "National Aeronautics and Space Administration", "NASA")

        result = tool.lookup_agencies("NASA")

        assert result["results"][0]["toptier_agency"]["toptier_code"] == "080"
        mock_embedding_generator.generate_embedding.assert_not_called()

    def test_prefix_match_skips_embedding_generation(self, tool, mock_embedding_generator):
        _make_matview_row("200", "National Archives and Records Administration", "NARA")

        result = tool.lookup_agencies("Nat")

        assert len(result["results"]) == 1
        mock_embedding_generator.generate_embedding.assert_not_called()

    def test_results_ordered_toptier_first_then_alphabetically(self, tool, mock_embedding_generator):
        _make_matview_row("200", "Zeta Bureau", toptier_flag=False)
        _make_matview_row("200", "Alpha Bureau", toptier_flag=True)
        _make_matview_row("200", "Beta Bureau", toptier_flag=True)

        result = tool.lookup_agencies("Bureau")

        names = [r["toptier_agency"]["name"] for r in result["results"]]
        assert names[0] == "Alpha Bureau"
        assert names[1] == "Beta Bureau"
        assert names[-1] == "Zeta Bureau"

    def test_top_k_truncates_results(self, tool, mock_embedding_generator):
        for i in range(5):
            _make_matview_row(f"20{i}", f"Prefix Agency {i}")

        result = tool.lookup_agencies("Prefix", top_k=2)

        assert len(result["results"]) == 2

    def test_query_is_stripped_before_matching(self, tool, mock_embedding_generator):
        _make_matview_row("080", "National Aeronautics and Space Administration", "NASA")

        result = tool.lookup_agencies("  NASA  ")

        assert result["results"][0]["toptier_agency"]["toptier_code"] == "080"
        mock_embedding_generator.generate_embedding.assert_not_called()


class TestHybridSearch:
    def test_falls_through_to_embedding_search(self, tool, mock_embedding_generator):
        dims = getattr(ToptierAgency, "embedding_dimensions", 256)
        toptier = _make_toptier_agency("999", "Space Exploration Agency", "SEA", embedding=_unit_vector(dims, 0))
        _make_agency(toptier)

        mock_embedding_generator.generate_embedding.return_value = _unit_vector(dims, 0)

        result = tool.lookup_agencies("orbital missions and rockets")

        codes = {r["toptier_agency"]["toptier_code"] for r in result["results"]}
        assert "999" in codes
        mock_embedding_generator.generate_embedding.assert_called_once()

    def test_vector_distance_threshold_excludes_far_matches(self, tool, mock_embedding_generator):
        dims = getattr(ToptierAgency, "embedding_dimensions", 256)
        close_top = _make_toptier_agency("111", "Close Match Agency", "CMA", embedding=_unit_vector(dims, 0))
        far_top = _make_toptier_agency("222", "Far Match Agency", "FMA", embedding=_unit_vector(dims, 1))
        _make_agency(close_top)
        _make_agency(far_top)

        mock_embedding_generator.generate_embedding.return_value = _unit_vector(dims, 0)

        result = tool.lookup_agencies("some non matching query text")

        codes = {r["toptier_agency"]["toptier_code"] for r in result["results"]}
        assert "111" in codes
        assert "222" not in codes

    def test_hybrid_score_orders_results_descending(self, tool, mock_embedding_generator):
        dims = getattr(ToptierAgency, "embedding_dimensions", 256)
        # Same embedding distance (0.0) for both; text similarity differs via name.
        high_top = _make_toptier_agency("300", "orbital research", "ORA", embedding=_unit_vector(dims, 0))
        low_top = _make_toptier_agency("400", "completely different naming", "CDN", embedding=_unit_vector(dims, 0))
        _make_agency(high_top)
        _make_agency(low_top)

        mock_embedding_generator.generate_embedding.return_value = _unit_vector(dims, 0)

        result = tool.lookup_agencies("orbital research")

        codes = [r["toptier_agency"]["toptier_code"] for r in result["results"]]
        assert codes[0] == "300"

    def test_top_k_truncates_hybrid_results(self, tool, mock_embedding_generator):
        dims = getattr(ToptierAgency, "embedding_dimensions", 256)
        mock_embedding_generator.generate_embedding.return_value = _unit_vector(dims, 0)

        for i in range(5):
            top = _make_toptier_agency(f"50{i}", f"Matching Agency {i}", f"MA{i}", embedding=_unit_vector(dims, 0))
            _make_agency(top)

        result = tool.lookup_agencies("matching agency query", top_k=2)

        assert len(result["results"]) == 2

    def test_hybrid_search_includes_subtier_agency_in_results(self, tool, mock_embedding_generator):
        dims = getattr(ToptierAgency, "embedding_dimensions", 256)
        top = _make_toptier_agency("600", "Department of Example", "DOE", embedding=_unit_vector(dims, 0))
        sub = _make_subtier_agency("Example Bureau", "EB", embedding=_unit_vector(dims, 0))
        _make_agency(top, subtier_agency=sub, toptier_flag=False)

        mock_embedding_generator.generate_embedding.return_value = _unit_vector(dims, 0)

        result = tool.lookup_agencies("example bureau semantics")

        subtier_names = {r["subtier_agency"]["name"] for r in result["results"]}
        assert "Example Bureau" in subtier_names


class TestMatviewRowToEntry:
    def test_maps_all_fields_correctly(self, tool):
        row = _make_matview_row(
            "080",
            "National Aeronautics and Space Administration",
            "NASA",
            subtier_name="NASA Subtier",
            subtier_abbreviation="NS",
        )

        entry = AgencyLookupTool._matview_row_to_entry(row)

        assert entry["id"] == row.agency_autocomplete_id
        assert entry["toptier_flag"] == row.toptier_flag
        assert entry["toptier_agency"] == {
            "toptier_code": "080",
            "abbreviation": "NASA",
            "name": "National Aeronautics and Space Administration",
        }
        assert entry["subtier_agency"] == {"abbreviation": "NS", "name": "NASA Subtier"}

    def test_handles_null_subtier_fields(self, tool):
        row = _make_matview_row("080", "National Aeronautics and Space Administration", "NASA")

        entry = AgencyLookupTool._matview_row_to_entry(row)

        assert entry["subtier_agency"] == {"abbreviation": None, "name": None}


class TestAgencyRowToEntry:
    def test_maps_toptier_and_subtier_agency(self, tool):
        top = _make_toptier_agency("080", "National Aeronautics and Space Administration", "NASA")
        sub = _make_subtier_agency("NASA Subtier", "NS")
        agency = _make_agency(top, subtier_agency=sub, toptier_flag=False)

        entry = AgencyLookupTool._agency_row_to_entry(agency)

        assert entry["id"] == agency.id
        assert entry["toptier_flag"] is False
        assert entry["toptier_agency"] == {
            "toptier_code": "080",
            "abbreviation": "NASA",
            "name": "National Aeronautics and Space Administration",
        }
        assert entry["subtier_agency"] == {"abbreviation": "NS", "name": "NASA Subtier"}

    def test_handles_missing_subtier_agency(self, tool):
        top = _make_toptier_agency("080", "National Aeronautics and Space Administration", "NASA")
        agency = _make_agency(top, subtier_agency=None, toptier_flag=True)

        entry = AgencyLookupTool._agency_row_to_entry(agency)

        assert entry["subtier_agency"] == {"abbreviation": None, "name": None}


class TestLookupAgencyToolRegistration:
    def test_tool_description_has_required_query_field(self):
        schema = lookup_agency_tool.description.input_schema
        assert "query" in schema["properties"]
        assert schema["required"] == ["query"]

    def test_tool_function_is_bound_lookup_agencies(self):
        result = lookup_agency_tool.function
        assert callable(result)
