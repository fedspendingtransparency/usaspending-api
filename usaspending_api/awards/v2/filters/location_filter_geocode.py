from django.apps import apps
from django.db.models import Q
from usaspending_api.common.exceptions import InvalidParameterException


LOCATION_MAPPING = {
    'country': 'location_country_code',
    'state': 'state_code',
    'county': 'county_code',
    'district': 'congressional_code',
    'zip': 'zip5',
}


def geocode_filter_locations(scope, values, model, use_matview=False, default_model='awards'):
    """
    Function filter querysets on location table
    scope- place of performance or recipient location mappings
    values- array of location requests
    model- awards or transactions will create queryset for model
    returns queryset
    """
    queryset_init = True
    or_queryset = None

    if type(model) == str:
        model = apps.get_model(default_model, model)

    nested_values = create_nested_object(values)

    for country, state_zip in nested_values.items():
        country_qs = Q(**{f'{scope}_country_code__exact': country})
        state_qs = Q()

        for state_zip_key, state_values in state_zip.items():
            if state_zip_key == 'zip':
                state_qs |= Q(**{f'{scope}_zip5__exact': state_values})
            else:
                state_inner_qs = Q(**{f'{scope}_state_code__exact': state_zip_key})

                if len(state_values['county']) > 0:
                    state_inner_qs &= Q(**{f'{scope}_county_code__in': state_values['county']})
                elif len(state_values['district']) > 0:
                    state_inner_qs &= Q(**{f'{scope}_congressional_code__in': state_values['district']})

                state_qs |= state_inner_qs

        country_qs &= state_qs

        if queryset_init:
            or_queryset = model.objects.filter(country_qs)
        else:
            or_queryset |= model.objects.filter(country_qs)
    return or_queryset


def create_nested_object(values):
    nested_locations = {}
    for v in values:

        if nested_locations.get(v['country']) is None:
            nested_locations[v['country']] = {}

        if 'zip' in v:
            nested_locations[v['country']]['zip'] = v['zip']
        elif 'state' in v and nested_locations[v['country']].get(v['state']) is None:
            nested_locations[v['country']][v['state']] = {'county': [], 'district': []}

        if v.get('county'):
                nested_locations[v['country']][v['state']]['county'].extend(get_fields_list('county', v['county']))
        elif v.get('district'):
                nested_locations[v['country']][v['state']]['district'].extend(
                    get_fields_list('district', v['district']))

    return nested_locations


def check_location_fields(fields):
    # Request must have country, and can only have 3 fields,
    # and must have state if there is county or district
    if 'country' not in fields or \
            ('state' not in fields and
                ('county' in fields or 'district' in fields)):

        raise InvalidParameterException(
            'Invalid filter: recipient has incorrect object.'
        )


def get_fields_list(scope, field_value,):
    """List of values to search for; `field_value`, plus possibly variants on it"""
    if scope in ['county', 'district']:
        try:
            return [str(int(field_value)), field_value, str(float(field_value))]
        except ValueError:
            # if filter causes an error when casting to a float or integer
            # Example: 'ZZ' for an area without a congressional code
            return [field_value]
    return [field_value]
