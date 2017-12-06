from django.apps import apps
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
    q_str, loc_dict = return_query_strings(use_matview)
    queryset_init = False
    or_queryset = None

    for v in values:
        fields = v.keys()

        check_location_fields(fields)

        kwargs = {}
        for loc_scope in fields:
            if loc_dict.get(loc_scope) is not None:
                key_str = q_str.format(scope, loc_dict.get(loc_scope))
                kwargs[key_str] = get_fields_list(loc_dict.get(loc_scope), v.get(loc_scope), loc_dict)

        if type(model) == str:
            model = apps.get_model(default_model, model)
        qs = model.objects.filter(**kwargs)

        if queryset_init:
            or_queryset |= qs
        else:
            queryset_init = True
            or_queryset = qs
    return or_queryset


def check_location_fields(fields):
    # Request must have country, and can only have 3 fields,
    # and must have state if there is county or district
    if 'country' not in fields or \
            ('state' not in fields and
                ('county' in fields or 'district' in fields)):

        raise InvalidParameterException(
            'Invalid filter: recipient has incorrect object.'
        )


def get_fields_list(scope, field_value, loc_dict):
    """List of values to search for; `field_value`, plus possibly variants on it"""
    if scope not in loc_dict.values():
        try:
            return [str(int(field_value)), field_value, str(float(field_value))]
        except ValueError:
            # if filter causes an error when casting to a float or integer
            # Example: 'ZZ' for an area without a congressional code
            return [field_value]
    return [field_value]


def return_query_strings(use_matview):
    # Returns query strings according based on mat view or database
    loc_dict = LOCATION_MAPPING
    q_str = '{0}__{1}__in'

    if use_matview:
        q_str = '{0}_{1}__in'
        loc_dict['country'] = 'country_code'

    return q_str, loc_dict
