"""Guidance that cannot be checked without a model call is pinned here, so that editing a prompt does not silently
drop it. The behavior itself is covered by the eval cases."""

from pathlib import Path

import yaml

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"


def _prompt_text(name: str) -> str:
    prompts = yaml.safe_load((FIXTURES / "prompts.yaml").read_text(encoding="utf8"))
    return " ".join(next(p for p in prompts if p["fields"]["name"] == name)["fields"]["text"].split())


def test_query_review_does_not_turn_an_agency_into_a_topic():
    text = _prompt_text("query-review-v1")

    assert 'Never turn it into "Awards mentioning <acronym>"' in text
    assert "full official name" in text
    assert "leave the acronym as written" in text
    assert "When the acronym is also an ordinary word, as ICE is" in text


def test_query_review_keeps_award_types_and_does_not_reword_aggregation_questions():
    text = _prompt_text("query-review-v1")

    assert 'Never replace a specific award type with the generic word "Awards"' in text
    assert "Do not reword them" in text
    assert "you are not told today's date" in text


def test_query_review_rejects_rarely_and_only_real_comparisons():
    text = _prompt_text("query-review-v1")

    assert "It is a last resort" in text
    assert 'Do not rewrite it as "Awards mentioning <topic>"' in text
    assert "A list of things to search together is not a comparison" in text
    assert "Never put a company, agency, person or place in the rewrite that the user did not name" in text


def test_query_review_spells_out_abbreviations_in_topic_rewrites():
    text = _prompt_text("query-review-v1")

    assert 'spell out any abbreviation in full: "AI" becomes "artificial intelligence"' in text
