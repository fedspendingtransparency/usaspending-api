"""
Convert a saved Smart Assist filter request into the filter format the search endpoints (and QueryWithFilters) use.

The filter hash stores the website's Redux filter shape (keyword dicts, selectedLocations keyed by identifier, etc.).
The search API takes a different shape, and the only converter lives in the website code, so this mirrors it for the
fields that Smart Assist can set (treasuryAccounts is not converted yet). Anything empty is left out. A malformed
filter raises (for example KeyError), and the caller treats that as "no result check" because the check is advisory.
"""

import copy
from datetime import date
from typing import Any

from usaspending_api.common.validator.award_filter import AWARD_FILTER_NO_RECIPIENT_ID
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.filters.time_period.decorators import NEW_AWARDS_ONLY_KEYWORD

_LOCATION_KEYS = ("country", "state", "county", "city", "district_original", "district_current", "zip")


def _fiscal_year_range(fiscal_year: str) -> dict[str, str]:
    """A federal fiscal year runs October 1 of the prior calendar year through September 30."""
    year = int(fiscal_year)
    return {"start_date": date(year - 1, 10, 1).isoformat(), "end_date": date(year, 9, 30).isoformat()}


def _values(value: Any) -> list:
    """The website stores some lists (keyword, selectedAwardIDs) as {item: item} dicts; accept either form."""
    if isinstance(value, dict):
        return list(value.keys())
    return list(value or [])


def _time_period(filters: dict) -> list[dict[str, str]]:
    if filters.get("timePeriodType") == "dr":
        ranges = [{"start_date": tp["start_date"], "end_date": tp["end_date"]} for tp in filters.get("time_period", [])]
    else:
        ranges = [_fiscal_year_range(fy) for fy in filters.get("timePeriodFY", [])]

    if filters.get("filterNewAwardsOnlySelected") and filters.get("filterNewAwardsOnlyActive"):
        for time_range in ranges:
            time_range["date_type"] = NEW_AWARDS_ONLY_KEYWORD
    return ranges


def _locations(selected: dict) -> list[dict[str, str]]:
    locations = []
    for location in (selected or {}).values():
        location_filter = location.get("filter", location)
        locations.append({key: location_filter[key] for key in _LOCATION_KEYS if location_filter.get(key)})
    return locations


def _agencies(filters: dict) -> list[dict[str, str]]:
    agencies = []
    for agency_type, field in (("awarding", "selectedAwardingAgencies"), ("funding", "selectedFundingAgencies")):
        for agency in (filters.get(field) or {}).values():
            tier = agency.get("agencyType", "toptier")
            toptier_name = agency["toptier_agency"]["name"]
            name = agency["subtier_agency"]["name"] if tier == "subtier" else toptier_name
            agencies.append({"type": agency_type, "tier": tier, "name": name, "toptier_name": toptier_name})
    return agencies


def _award_amounts(amounts: dict) -> list[dict[str, int]]:
    """An open-ended range ("over $1 million") leaves out the missing bound; the search API rejects a None bound."""
    bounds = ({"lower_bound": lower, "upper_bound": upper} for lower, upper in (amounts or {}).values())
    return [{key: value for key, value in bound.items() if value is not None} for bound in bounds]


def _code_object(codes: dict | None) -> dict[str, list]:
    """A {require, exclude} object with only the non-empty parts; empty when nothing is selected."""
    return {key: codes[key] for key in ("require", "exclude") if codes and codes.get(key)}


def validate_for_search_api(filter_request: dict) -> None:
    """Raise the search API's own exception if its endpoints would reject these filters.

    The website sends the saved filters to the search endpoints, which validate them (keywords of at least 3 characters,
    dates no earlier than the API's minimum, number formats). Checking here, before the filters are saved, lets the
    model fix them instead of the results page failing with a 422.
    """
    TinyShield(copy.deepcopy(AWARD_FILTER_NO_RECIPIENT_ID)).block({"filters": to_search_filters(filter_request)})


def to_search_filters(filter_request: dict) -> dict[str, Any]:
    """Convert a saved filter request (with or without its outer {"filters": ...} wrapper) to search filters."""
    filters = filter_request.get("filters", filter_request)

    converted: dict[str, Any] = {
        "keywords": _values(filters.get("keyword")),
        "description": filters.get("awardDescription"),
        "time_period": _time_period(filters),
        "place_of_performance_locations": _locations(filters.get("selectedLocations")),
        "place_of_performance_scope": "foreign" if filters.get("locationDomesticForeign") == "foreign" else None,
        "recipient_locations": _locations(filters.get("selectedRecipientLocations")),
        "recipient_scope": "foreign" if filters.get("recipientDomesticForeign") == "foreign" else None,
        "agencies": _agencies(filters),
        "recipient_search_text": list(filters.get("selectedRecipients") or []),
        "recipient_type_names": list(filters.get("recipientType") or []),
        "award_type_codes": list(filters.get("awardType") or []),
        "award_ids": _values(filters.get("selectedAwardIDs")),
        "award_amounts": _award_amounts(filters.get("awardAmounts")),
        "program_numbers": list((filters.get("selectedCFDA") or {}).keys()),
        "naics_codes": _code_object(filters.get("naicsCodes")),
        "psc_codes": _code_object(filters.get("pscCodes")),
        "tas_codes": _code_object(filters.get("tasCodes")),
        "def_codes": list((filters.get("defCodes") or {}).get("require") or []) or list(filters.get("defCode") or []),
        "contract_pricing_type_codes": list(filters.get("pricingType") or []),
        "set_aside_type_codes": list(filters.get("setAside") or []),
        "extent_competed_type_codes": list(filters.get("extentCompeted") or []),
    }
    return {key: value for key, value in converted.items() if value}
