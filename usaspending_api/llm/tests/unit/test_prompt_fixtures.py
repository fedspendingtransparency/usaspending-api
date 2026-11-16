"""Guidance that cannot be checked without a model call is pinned here, so that editing a prompt does not silently
drop it. The behavior itself is covered by the eval cases."""

from pathlib import Path

import yaml

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"


def _prompt_text(name: str) -> str:
    prompts = yaml.safe_load((FIXTURES / "prompts.yaml").read_text(encoding="utf8"))
    return " ".join(next(p for p in prompts if p["fields"]["name"] == name)["fields"]["text"].split())


def test_filter_search_treats_agency_acronyms_as_agencies_and_checks_before_using_a_keyword():
    text = _prompt_text("filter-preference-v3")

    assert "named in the query, by its name or its acronym, is an agency filter, not a keyword" in text
    assert "ICE" in text
    assert "Before using an acronym or proper name as a keyword, call lookup_agencies" in text
    assert "its own subtier entry" in text


def test_query_review_does_not_turn_an_agency_into_a_topic():
    text = _prompt_text("query-review-v1")

    assert 'Never turn it into "Awards mentioning <acronym>"' in text
    assert "full official name" in text
    assert "leave the acronym as written" in text
    assert "When the acronym is also an ordinary word, as ICE is" in text


def test_query_review_spells_out_abbreviations_in_topic_rewrites():
    text = _prompt_text("query-review-v1")

    assert 'spell out any abbreviation in full: "AI" becomes "artificial intelligence"' in text
