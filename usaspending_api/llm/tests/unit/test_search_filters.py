import json
from datetime import date

import pytest
from pydantic import ValidationError

from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.llm.evals.loader import _parse_case, dataset_directory
from usaspending_api.llm.tools.helpers.search_filters import to_search_filters, validate_for_search_api
from usaspending_api.search.filters.elasticsearch.filter import QueryType

TEXAS = {
    "identifier": "USA_TX",
    "filter": {"country": "USA", "state": "TX", "county": None, "city": None},
    "display": {"entity": "State", "standalone": "Texas", "title": "TEXAS"},
}
DOD_TOPTIER = {
    "id": 1,
    "toptier_flag": True,
    "agencyType": "toptier",
    "toptier_agency": {"id": 1, "toptier_code": "097", "abbreviation": "DOD", "name": "Department of Defense"},
    "subtier_agency": {"abbreviation": "DOD", "name": "Department of Defense"},
}
ARMY_SUBTIER = {
    "id": 2,
    "toptier_flag": False,
    "agencyType": "subtier",
    "toptier_agency": {"id": 1, "toptier_code": "097", "abbreviation": "DOD", "name": "Department of Defense"},
    "subtier_agency": {"abbreviation": "USA", "name": "Department of the Army"},
}


def test_empty_filters_convert_to_nothing():
    assert to_search_filters({"filters": {"keyword": {}, "timePeriodFY": [], "awardType": []}}) == {}


def test_accepts_wrapped_and_unwrapped_requests():
    assert to_search_filters({"filters": {"keyword": ["a"]}}) == to_search_filters({"keyword": ["a"]})


def test_keywords_from_dict_or_list():
    assert to_search_filters({"keyword": {"housing": "housing", "affordable": "affordable"}}) == {
        "keywords": ["housing", "affordable"]
    }
    assert to_search_filters({"keyword": ["housing"]}) == {"keywords": ["housing"]}


def test_award_description():
    assert to_search_filters({"awardDescription": "artificial intelligence"}) == {
        "description": "artificial intelligence"
    }


def test_fiscal_years_become_october_to_september_ranges():
    assert to_search_filters({"timePeriodType": "fy", "timePeriodFY": ["2022", "2024"]}) == {
        "time_period": [
            {"start_date": "2021-10-01", "end_date": "2022-09-30"},
            {"start_date": "2023-10-01", "end_date": "2024-09-30"},
        ]
    }


def test_date_ranges_pass_through():
    date_range = {"start_date": "2024-01-01", "end_date": "2024-12-31"}
    assert to_search_filters({"timePeriodType": "dr", "time_period": [date_range]}) == {"time_period": [date_range]}


def test_new_awards_only_adds_date_type_to_each_range():
    result = to_search_filters(
        {
            "timePeriodType": "fy",
            "timePeriodFY": ["2025"],
            "filterNewAwardsOnlySelected": True,
            "filterNewAwardsOnlyActive": True,
        }
    )
    assert result["time_period"][0]["date_type"] == "new_awards_only"


def test_locations_drop_empty_parts_and_scope_only_when_foreign():
    result = to_search_filters(
        {
            "selectedLocations": {"USA_TX": TEXAS},
            "locationDomesticForeign": "all",
            "recipientDomesticForeign": "foreign",
        }
    )
    assert result == {
        "place_of_performance_locations": [{"country": "USA", "state": "TX"}],
        "recipient_scope": "foreign",
    }


def test_agencies_toptier_and_subtier():
    result = to_search_filters(
        {"selectedAwardingAgencies": {"1_toptier": DOD_TOPTIER}, "selectedFundingAgencies": {"2_subtier": ARMY_SUBTIER}}
    )
    assert result["agencies"] == [
        {
            "type": "awarding",
            "tier": "toptier",
            "name": "Department of Defense",
            "toptier_name": "Department of Defense",
        },
        {
            "type": "funding",
            "tier": "subtier",
            "name": "Department of the Army",
            "toptier_name": "Department of Defense",
        },
    ]


def test_amounts_become_bounds():
    result = to_search_filters({"awardAmounts": {"range-1": [1000000, 25000000], "range-4": [500000000, None]}})
    assert result == {
        "award_amounts": [
            {"lower_bound": 1000000, "upper_bound": 25000000},
            {"lower_bound": 500000000},  # open-ended: the missing bound is left out, never None
        ]
    }


def test_codes_and_simple_lists():
    result = to_search_filters(
        {
            "selectedRecipients": ["BOEING CO"],
            "recipientType": ["higher_education"],
            "awardType": ["A", "B"],
            "selectedAwardIDs": {"ABC123": "ABC123"},
            "selectedCFDA": {"10.551": {"identifier": "10.551", "program_number": "10.551", "program_title": "SNAP"}},
            "naicsCodes": {"require": ["541511"], "exclude": [], "counts": [{"value": "54"}]},
            "pscCodes": {"require": [["Service", "R", "R4"]], "exclude": []},
            "defCodes": {"require": ["L", "M"], "exclude": []},
            "pricingType": ["J"],
            "setAside": ["SBA"],
            "extentCompeted": ["A"],
        }
    )
    assert result == {
        "recipient_search_text": ["BOEING CO"],
        "recipient_type_names": ["higher_education"],
        "award_type_codes": ["A", "B"],
        "award_ids": ["ABC123"],
        "program_numbers": ["10.551"],
        "naics_codes": {"require": ["541511"]},
        "psc_codes": {"require": [["Service", "R", "R4"]]},
        "def_codes": ["L", "M"],
        "contract_pricing_type_codes": ["J"],
        "set_aside_type_codes": ["SBA"],
        "extent_competed_type_codes": ["A"],
    }


@pytest.mark.django_db
def test_converted_filters_are_accepted_by_query_with_filters():
    """The key names and shapes must be ones the search query builder actually understands."""
    converted = to_search_filters(
        {
            "keyword": {"housing": "housing"},
            "awardDescription": "artificial intelligence",
            "timePeriodType": "fy",
            "timePeriodFY": ["2022"],
            "selectedLocations": {"USA_TX": TEXAS},
            "awardType": ["A", "B"],
            "awardAmounts": {"range-1": [1000000, 25000000]},
            "naicsCodes": {"require": ["541511"], "exclude": []},
            "selectedRecipients": ["BOEING CO"],
            "defCodes": {"require": ["L"], "exclude": []},
        }
    )
    assert QueryWithFilters(QueryType.AWARDS).generate_elasticsearch_query(converted)


@pytest.mark.parametrize("bad", [{"selectedAwardingAgencies": {"1": {"agencyType": "toptier"}}}])
def test_unconvertible_filters_raise_so_the_caller_can_skip_the_check(bad):
    with pytest.raises(KeyError):
        to_search_filters(bad)


# Eval cases whose expected filters are themselves rejected by the search API, so they are skipped here and need fixing
# in the dataset: case 36 expects the 2-character keyword "AI", but keywords must be at least 3 characters.
KNOWN_INVALID_EXPECTED_FILTERS = {"36"}


@pytest.mark.django_db
def test_every_eval_case_converts_to_filters_the_search_api_accepts():
    """Guards the converter: what Smart Assist saves has to be accepted by the endpoints the website sends it to."""
    rows = json.loads((dataset_directory() / "config.json").read_text(encoding="utf8"))
    seen_ids: set[str] = set()
    checked, rejected = 0, []
    for number, row in enumerate(rows, start=1):
        try:
            case = _parse_case(row, number, seen_ids, date.today())
        except ValidationError:
            continue  # needs reference data (DEFC codes, NAICS/PSC counts) that an empty test database does not have
        if case.name in KNOWN_INVALID_EXPECTED_FILTERS or not to_search_filters({"filters": case.expected_output}):
            continue
        checked += 1
        try:
            validate_for_search_api({"filters": case.expected_output})
        except Exception as exc:
            rejected.append((case.name, str(getattr(exc, "detail", exc))))

    assert checked > 100  # most cases do not depend on reference data, so this still covers the converter broadly
    assert rejected == []
