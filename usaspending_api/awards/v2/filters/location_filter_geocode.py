from typing import Any

from django.db.models import Q

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.api_helper import (
    DUPLICATE_DISTRICT_LOCATION_PARAMETERS,
    INCOMPATIBLE_DISTRICT_LOCATION_PARAMETERS,
)
from usaspending_api.common.helpers.dict_helpers import upper_case_dict_values

ALL_FOREIGN_COUNTRIES = "FOREIGN"


def geocode_filter_locations(scope: str, values: list[dict[str, Any]]) -> Q:
    """
    Function filter querysets on location table
    scope- place of performance or recipient location mappings
    values- array of location requests
    returns queryset
    """
    nested_values = create_nested_object(values)

    or_queryset = Q()
    for country, state_zip in nested_values.items():
        country_qs = _build_country_query(scope, country)
        state_qs = _build_state_queries(scope, state_zip)

        or_queryset |= (country_qs & state_qs) if country_qs else state_qs

    return or_queryset


def _build_country_query(scope: str, country: str) -> Q | None:
    """Build country-level query filter"""
    if country == ALL_FOREIGN_COUNTRIES:
        return None
    return Q(**{f"{scope}_country_code__exact": country})


def _build_state_queries(scope: str, state_zip: dict[str, Any]) -> Q:
    """Build state-level query filters"""
    state_qs = Q()

    for state_zip_key, location_values in state_zip.items():
        if state_zip_key == "city":
            state_inner_qs = Q(**{f"{scope}_city_name__in": location_values})
        elif state_zip_key == "zip":
            state_inner_qs = Q(**{f"{scope}_zip5__in": location_values})
        else:
            state_inner_qs = _build_state_location_query(scope, state_zip_key, location_values)

        state_qs |= state_inner_qs

    return state_qs


def _build_state_location_query(scope: str, state_code: str, location_values: dict[str, Any]) -> Q:
    """Build query for state with nested county/district/city filters"""
    state_qs = Q(**{f"{scope}_state_code__exact": state_code.upper()})

    # Build sub-filters
    sub_filters = Q()

    if location_values.get("county"):
        sub_filters |= Q(**{f"{scope}_county_code__in": location_values["county"]})

    if location_values.get("district_current"):
        sub_filters |= Q(**{f"{scope}_congressional_code_current__in": location_values["district_current"]})

    if location_values.get("district_original"):
        sub_filters |= Q(**{f"{scope}_congressional_code__in": location_values["district_original"]})

    if location_values.get("city"):
        sub_filters |= Q(**{f"{scope}_city_name__in": location_values["city"]})

    return state_qs & sub_filters


def validate_location_keys(values: list[dict[str, Any]]) -> None:
    """Validate that the keys provided are sufficient and match properly."""
    for v in values:
        state = v.get("state")
        country = v.get("country")
        county = v.get("county")
        district_current = v.get("district_current")
        district_original = v.get("district_original")
        if (state is None or country != "USA" or county is not None) and (
            district_current is not None or district_original is not None
        ):
            raise InvalidParameterException(INCOMPATIBLE_DISTRICT_LOCATION_PARAMETERS)
        if district_current is not None and district_original is not None:
            raise InvalidParameterException(DUPLICATE_DISTRICT_LOCATION_PARAMETERS)
        if ("country" not in v) or ("county" in v and "state" not in v):
            location_error_handling(v.keys())


def create_nested_object(values: list[dict[str, Any]]) -> dict[str, Any]:
    """Makes sure keys provided are valid"""
    validate_location_keys(values)

    nested_locations: dict[str, Any] = {}
    for v in values:
        upper_case_dict_values(v)
        location_data = _extract_location_data(v)
        _process_location(nested_locations, location_data)

    return nested_locations


def _extract_location_data(location_dict: dict[str, Any]) -> dict[str, Any]:
    """Extract location fields from the input dictionary"""
    return {
        "city": location_dict.get("city"),
        "country": location_dict.get("country"),
        "county": location_dict.get("county"),
        "district_original": location_dict.get("district_original"),
        "district_current": location_dict.get("district_current"),
        "state": location_dict.get("state"),
        "zip_code": location_dict.get("zip"),
    }


def _process_location(nested_locations: dict[str, Any], location_data: dict[str, Any]) -> None:
    """Process a single location and add it to the nested structure"""
    country = location_data["country"]

    # Initialize country if needed
    if country not in nested_locations:
        nested_locations[country] = {}

    # Process zip codes
    _process_zip_code(nested_locations[country], location_data["zip_code"])

    # Process city (country-level)
    _process_country_level_city(nested_locations[country], location_data["city"])

    # Process state-level data
    if location_data["state"]:
        _process_state_data(nested_locations[country], location_data)


def _process_zip_code(country_data: dict[str, Any], zip_code: str | None) -> None:
    """Process zip code for a country"""
    if not zip_code:
        return

    if "zip" not in country_data:
        country_data["zip"] = []

    country_data["zip"].append(zip_code)


def _process_country_level_city(country_data: dict[str, Any], city: str | None) -> None:
    """Initialize city list at country level if needed"""
    if city and "city" not in country_data:
        country_data["city"] = []


def _process_state_data(country_data: dict[str, Any], location_data: dict[str, Any]) -> None:
    """Process state-level location data"""
    state = location_data["state"]

    # Initialize state structure if needed
    if state not in country_data:
        country_data[state] = {
            "county": [],
            "district_original": [],
            "district_current": [],
            "city": [],
        }

    state_data = country_data[state]

    # Add county data
    if location_data["county"]:
        state_data["county"].extend(get_fields_list("county", location_data["county"]))

    # Add district data
    if location_data["district_current"]:
        state_data["district_current"].extend(
            get_fields_list("district_current", location_data["district_current"])
        )

    if location_data["district_original"]:
        state_data["district_original"].extend(
            get_fields_list("district_original", location_data["district_original"])
        )

    # Add city data
    if location_data["city"]:
        state_data["city"].append(location_data["city"])
    elif location_data["city"] and not state:
        # City without state goes to country level
        country_data["city"].append(location_data["city"])


def location_error_handling(fields: Any) -> None:
    """Raise the relevant error for location keys."""
    # Request must have country, and can only have 3 fields, and must have state if there is county
    if "country" not in fields:
        raise InvalidParameterException("Invalid filter:  Missing necessary location field: country.")

    if "state" not in fields and ("county" in fields):
        raise InvalidParameterException("Invalid filter:  Missing necessary location field: state.")


def get_fields_list(scope: str, field_value: str) -> list[str]:
    """List of values to search for; `field_value`, plus possibly variants on it"""
    if scope in ["congressional_code", "county_code"]:
        try:
            # Congressional and county codes are not uniform and contain multiple variables
            # In the location table Ex congressional code (01): '01', '1.0', '1'
            return [str(int(field_value)), field_value, str(float(field_value))]
        except ValueError:
            # if filter causes an error when casting to a float or integer
            # Example: 'ZZ' for an area without a congressional code
            pass
    return [field_value]
