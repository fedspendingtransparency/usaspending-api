from django.apps import apps
from django.db.models import Q
from usaspending_api.common.exceptions import InvalidParameterException


def geocode_filter_locations(scope, values, model, use_matview=False, app_name='awards'):
    """
    Function filter querysets on location table
    scope- place of performance or recipient location mappings
    values- array of location requests
    model- awards or transactions will create queryset for model
    returns queryset
    """
    or_queryset = Q()

    # Accounts for differences between matview queries and regular queries
    q_str, country_code = return_query_string(use_matview)

    if type(model) == str:
        model = apps.get_model(app_name, model)

    nested_values = create_nested_object(values)

    for country, state_zip in nested_values.items():
        country_qs = Q(**{q_str.format(scope, country_code) + '__exact': country})
        state_qs = Q()

        for state_zip_key, state_values in state_zip.items():
            if state_zip_key == 'zip':
                state_inner_qs = Q(**{q_str.format(scope, 'zip5') + '__exact': state_values})
            else:
                state_inner_qs = Q(**{q_str.format(scope, 'state_code') + '__exact': state_zip_key})
                county_qs = Q()
                district_qs = Q()

                if state_values['county']:
                    county_qs = Q(**{q_str.format(scope, 'county_code') + '__in': state_values['county']})
                if state_values['district']:
                    district_qs = Q(**{q_str.format(scope, 'congressional_code') + '__in': state_values['district']})
                state_inner_qs &= (county_qs | district_qs)

            state_qs |= state_inner_qs

        or_queryset |= (country_qs & state_qs)
    return model.objects.filter(or_queryset)


def create_nested_object(values):
    nested_locations = {}
    for v in values:
        try:
            # First level in location filtering in country
            # All location requests must have a country otherwise there will be a key error
            if nested_locations.get(v['country']) is None:
                nested_locations[v['country']] = {}

            # Second level of filtering is zip and state
            # Requests must have a country+zip or country+state combination
            if 'zip' in v:
                nested_locations[v['country']]['zip'] = v['zip']
            elif 'state' in v and nested_locations[v['country']].get(v['state']) is None:
                nested_locations[v['country']][v['state']] = {'county': [], 'district': []}

            # Under state a user can filter on county and/or district (congressional_code)
            # Error will be thrown if no state value
            if v.get('county'):
                nested_locations[v['country']][v['state']]['county'].extend(get_fields_list('county', v['county']))

            if v.get('district'):
                nested_locations[v['country']][v['state']]['district'].extend(
                    get_fields_list('district', v['district']))

        except KeyError:
            # This result if there is value for country or state where necessary
            location_error_handling(v.keys())

    return nested_locations


def location_error_handling(fields):
    # Request must have country, and can only have 3 fields,
    # and must have state if there is county or district
    if 'country' not in fields:

        raise InvalidParameterException(
            'Invalid filter:  Missing necessary location field: country.'
        )
    elif 'state' not in fields and('county' in fields or 'district' in fields):
        raise InvalidParameterException(
            'Invalid filter:  Missing necessary location field: state.'
        )


def get_fields_list(location_filter_key, field_value,):
    """List of values to search for; `field_value`, plus possibly variants on it
       Location filter can be the keys in the LOCATION_MAPPING
    """
    if location_filter_key in ['county', 'district']:
        try:
            # Congressional and county codes are not uniform and contain multiple variables
            # In the location table Ex congressional code (01): '01', '1.0', '1'
            return [str(int(field_value)), field_value, str(float(field_value))]
        except ValueError:
            # if filter causes an error when casting to a float or integer
            # Example: 'ZZ' for an area without a congressional code
            return [field_value]
    return [field_value]


def return_query_string(use_matview):
    # Returns query strings according based on mat view or database

    if use_matview:
        # Queries going through the references_location table will not require a join
        # Example "pop__county_code"
        q_str = '{0}_{1}'
        # Matviews use country_code ex: pop_country_code
        country_code = 'country_code'
    else:
        # Queries going through the references_location table will a join in the filter
        # Example "place_of_performance__county_code"
        q_str = '{0}__{1}'
        # References_location table uses the col location_country_code
        country_code = 'location_country_code'

    return q_str, country_code
