import json
from pathlib import Path

import yaml

from usaspending_api.llm.assistants.filter_explain import ASSISTANT_NAME, build_explanation_prompt

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
FILTER_REQUEST = {
    "filters": {
        "keyword": {"housing": "housing"},
        "timePeriodType": "fy",
        "timePeriodFY": ["2022"],
        "selectedLocations": {
            "USA_NY": {
                "identifier": "USA_NY",
                "filter": {"country": "USA", "state": "NY"},
                "display": {"entity": "State", "standalone": "New York", "title": "NEW YORK"},
            }
        },
    }
}


def _fixture(name: str) -> list[dict]:
    return yaml.safe_load((FIXTURES / name).read_text(encoding="utf8"))


def test_prompt_contains_the_query_and_the_filters_in_search_format():
    prompt = build_explanation_prompt("affordable housing in New York in 2022", FILTER_REQUEST)

    assert prompt.startswith("User's search: affordable housing in New York in 2022")
    filters = json.loads(prompt.split("Filters applied:\n", 1)[1])
    assert filters == {
        "keywords": ["housing"],
        "time_period": {
            "selected_as": "fiscal year",
            "fiscal_years": {"2022": "October 1, 2021 to September 30, 2022"},
        },
        "place_of_performance_locations": [{"country": "USA", "state": "NY"}],
    }


def test_prompt_leaves_out_ui_bookkeeping():
    prompt = build_explanation_prompt("housing", FILTER_REQUEST)

    assert "display" not in prompt
    assert "timePeriodType" not in prompt


def test_fixtures_define_the_filter_explain_assistant_and_its_prompt():
    assistant = next(a for a in _fixture("assistants.yaml") if a["fields"]["name"] == ASSISTANT_NAME)
    prompt = next(p for p in _fixture("prompts.yaml") if p["pk"] == assistant["fields"]["system_prompt"])

    assert assistant["fields"]["is_active"] is True
    assert assistant["fields"]["inference_config"]["temperature"] == 0.0
    assert prompt["fields"]["name"] == "filter-explain-v1"


def test_the_prompt_names_the_product_and_avoids_totals_and_and_for_keywords():
    assistant = next(a for a in _fixture("assistants.yaml") if a["fields"]["name"] == ASSISTANT_NAME)
    text = next(p for p in _fixture("prompts.yaml") if p["pk"] == assistant["fields"]["system_prompt"])["fields"][
        "text"
    ]

    assert "Smart Assist" in text
    assert 'Never write "the tool"' in text
    assert "add anything up" in text
    assert 'join them with "or", never "and"' in text


def test_a_fiscal_year_is_described_as_a_fiscal_year_with_its_dates_and_not_as_a_date_range():
    prompt = build_explanation_prompt(
        "housing in 2025", {"filters": {"timePeriodType": "fy", "timePeriodFY": ["2025"]}}
    )

    time_period = json.loads(prompt.split("Filters applied:\n", 1)[1])["time_period"]
    assert time_period == {
        "selected_as": "fiscal year",
        "fiscal_years": {"2025": "October 1, 2024 to September 30, 2025"},
    }
    assert "start_date" not in prompt


def test_a_date_range_is_described_as_a_date_range():
    filters = {
        "timePeriodType": "dr",
        "time_period": [{"start_date": "2025-01-01", "end_date": "2025-03-31"}],
    }

    prompt = build_explanation_prompt("housing last quarter", {"filters": filters})

    time_period = json.loads(prompt.split("Filters applied:\n", 1)[1])["time_period"]
    assert time_period == {"selected_as": "date range", "date_ranges": ["2025-01-01 to 2025-03-31"]}


def test_no_time_period_is_left_out():
    prompt = build_explanation_prompt("housing", {"filters": {"keyword": {"housing": "housing"}}})

    assert "time_period" not in prompt


def test_the_prompt_tells_the_model_to_say_fiscal_year_and_never_date_range_for_one():
    assistant = next(a for a in _fixture("assistants.yaml") if a["fields"]["name"] == ASSISTANT_NAME)
    text = next(p for p in _fixture("prompts.yaml") if p["pk"] == assistant["fields"]["system_prompt"])["fields"][
        "text"
    ]

    assert 'When selected_as is "fiscal year"' in text
    assert 'Never call a fiscal year a "date range"' in text
