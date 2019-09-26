from django.db.models import Q

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.dict_helpers import upper_case_dict_values

ALL_FOREIGN_COUNTRIES = "FOREIGN"


def geocode_filter_locations(scope: str, values: list, use_matview: bool = False) -> Q:
    """
    Function filter querysets on location table
    scope- place of performance or recipient location mappings
    values- array of location requests
    returns queryset
    """
    or_queryset = Q()

    # Accounts for differences between matview queries and regular queries
    q_str, country_code = return_query_string(use_matview)

    # creates a dictionary with all of the locations organized by country
    # Counties and congressional districts are nested under state codes
    nested_values = create_nested_object(values)

    # In this for-loop a django Q filter object is created from the python dict
    for country, state_zip in nested_values.items():
        country_qs = None
        if country != ALL_FOREIGN_COUNTRIES:
            country_qs = Q(**{q_str.format(scope, country_code) + "__exact": country})
        state_qs = Q()

        for state_zip_key, location_values in state_zip.items():

            if state_zip_key == "city":
                state_inner_qs = Q(**{q_str.format(scope, "city_name") + "__in": location_values})
            elif state_zip_key == "zip":
                state_inner_qs = Q(**{q_str.format(scope, "zip5") + "__in": location_values})
            else:
                state_inner_qs = Q(**{q_str.format(scope, "state_code") + "__exact": state_zip_key.upper()})
                county_qs = Q()
                district_qs = Q()
                city_qs = Q()

                if location_values["county"]:
                    county_qs = Q(**{q_str.format(scope, "county_code") + "__in": location_values["county"]})
                if location_values["district"]:
                    district_qs = Q(**{q_str.format(scope, "congressional_code") + "__in": location_values["district"]})
                if location_values["city"]:
                    city_qs = Q(**{q_str.format(scope, "city_name") + "__in": location_values["city"]})
                state_inner_qs &= county_qs | district_qs | city_qs

            state_qs |= state_inner_qs
        if country_qs:
            or_queryset |= country_qs & state_qs
        else:
            or_queryset |= state_qs
    return or_queryset


def validate_location_keys(values):
    """ Validate that the keys provided are sufficient and match properly. """
    for v in values:
        if ("country" not in v) or (("district" in v or "county" in v) and "state" not in v):
            location_error_handling(v.keys())


def create_nested_object(values):
    """ Makes sure keys provided are valid """
    validate_location_keys(values)

    nested_locations = {}
    for v in values:
        upper_case_dict_values(v)
        city = v.get("city")
        country = v.get("country")
        county = v.get("county")
        district = v.get("district")
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
            nested_locations[country][state] = {"county": [], "district": [], "city": []}

        # Based on previous checks, there will always be a state if either of these exist
        if county:
            nested_locations[country][state]["county"].extend(get_fields_list("county", county))

        if district:
            nested_locations[country][state]["district"].extend(get_fields_list("district", district))

        if city and state:
            nested_locations[country][state]["city"].append(city)
        elif city:
            nested_locations[country]["city"].append(city)

    return nested_locations


def location_error_handling(fields):
    """ Raise the relevant error for location keys. """
    # Request must have country, and can only have 3 fields, and must have state if there is county or district
    if "country" not in fields:
        raise InvalidParameterException("Invalid filter:  Missing necessary location field: country.")

    if "state" not in fields and ("county" in fields or "district" in fields):
        raise InvalidParameterException("Invalid filter:  Missing necessary location field: state.")


def get_fields_list(scope, field_value):
    """ List of values to search for; `field_value`, plus possibly variants on it """
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


def return_query_string(use_matview: bool) -> tuple:
    """ Returns query strings based upon if the queryset is for a normalized or de-normalized model """

    if use_matview:  # de-normalized
        # Queries going through the references_location table will not require a join
        # Example "pop__county_code"
        q_str = "{0}_{1}"
        country_code_col = "country_code"  # Matviews use country_code ex: pop_country_code
    else:
        # Queries going through the references_location table will require a join in the filter
        # Example "place_of_performance__county_code"
        q_str = "{0}__{1}"
        country_code_col = "location_country_code"  # References_location table uses the col location_country_code

    return q_str, country_code_col
