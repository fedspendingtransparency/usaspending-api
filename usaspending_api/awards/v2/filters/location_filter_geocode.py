from django.db.models import Q

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.api_helper import (
    DUPLICATE_DISTRICT_LOCATION_PARAMETERS,
    INCOMPATIBLE_DISTRICT_LOCATION_PARAMETERS,
)
from usaspending_api.common.helpers.dict_helpers import upper_case_dict_values

ALL_FOREIGN_COUNTRIES = "FOREIGN"


def geocode_filter_locations(scope: str, values: list) -> Q:
    """
    Function filter querysets on location table
    scope- place of performance or recipient location mappings
    values- array of location requests
    returns queryset
    """
    or_queryset = Q()

    # creates a dictionary with all of the locations organized by country
    # Counties and congressional districts are nested under state codes
    nested_values = create_nested_object(values)

    # In this for-loop a django Q filter object is created from the python dict
    for country, state_zip in nested_values.items():
        country_qs = None
        if country != ALL_FOREIGN_COUNTRIES:
            country_qs = Q(**{f"{scope}_country_code__exact": country})
        state_qs = Q()

        for state_zip_key, location_values in state_zip.items():

            if state_zip_key == "city":
                state_inner_qs = Q(**{f"{scope}_city_name__in": location_values})
            elif state_zip_key == "zip":
                state_inner_qs = Q(**{f"{scope}_zip5__in": location_values})
            else:
                state_inner_qs = Q(**{f"{scope}_state_code__exact": state_zip_key.upper()})
                county_qs = Q()
                district_qs = Q()
                city_qs = Q()

                if location_values["county"]:
                    county_qs = Q(**{f"{scope}_county_code__in": location_values["county"]})
                if location_values["district_current"]:
                    district_qs = Q(**{f"{scope}_congressional_code_current__in": location_values["district_current"]})
                if location_values["district_original"]:
                    district_qs = Q(**{f"{scope}_congressional_code__in": location_values["district_original"]})
                if location_values["city"]:
                    city_qs = Q(**{f"{scope}_city_name__in": location_values["city"]})
                state_inner_qs &= county_qs | district_qs | city_qs

            state_qs |= state_inner_qs
        if country_qs:
            or_queryset |= country_qs & state_qs
        else:
            or_queryset |= state_qs
    return or_queryset


def validate_location_keys(values):
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


def create_nested_object(values):
    """Makes sure keys provided are valid"""
    validate_location_keys(values)

    nested_locations = {}
    for v in values:
        upper_case_dict_values(v)
        city = v.get("city")
        country = v.get("country")
        county = v.get("county")
        district_original = v.get("district_original")
        district_current = v.get("district_current")
        state = v.get("state")
        zip = v.get("zip")
        # First level in location filtering in country
        # All location requests must have a country otherwise there will be a key error
        if nested_locations.get(country) is None:
            nested_locations[country] = {}

        # Initialize the list
        if zip and not nested_locations[country].get("zip"):
            nested_locations[country]["zip"] = []

        if city and not nested_locations[country].get("city"):
            nested_locations[country]["city"] = []

        # Second level of filtering is zip and state
        # Requests must have a country+zip or country+state combination
        if zip:
            # Appending zips so we don't overwrite
            nested_locations[country]["zip"].append(zip)

        # If we have a state, add it to the list
        if state and nested_locations[country].get(state) is None:
            nested_locations[country][state] = {
                "county": [],
                "district_original": [],
                "district_current": [],
                "city": [],
            }

        # Based on previous checks, there will always be a state if either of these exist
        if county:
            nested_locations[country][state]["county"].extend(get_fields_list("county", county))

        if district_current:
            nested_locations[country][state]["district_current"].extend(
                get_fields_list("district_current", district_current)
            )

        if district_original:
            nested_locations[country][state]["district_original"].extend(
                get_fields_list("district_original", district_original)
            )

        if city and state:
            nested_locations[country][state]["city"].append(city)
        elif city:
            nested_locations[country]["city"].append(city)

    return nested_locations


def location_error_handling(fields):
    """Raise the relevant error for location keys."""
    # Request must have country, and can only have 3 fields, and must have state if there is county
    if "country" not in fields:
        raise InvalidParameterException("Invalid filter:  Missing necessary location field: country.")

    if "state" not in fields and ("county" in fields):
        raise InvalidParameterException("Invalid filter:  Missing necessary location field: state.")


def get_fields_list(scope, field_value):
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
