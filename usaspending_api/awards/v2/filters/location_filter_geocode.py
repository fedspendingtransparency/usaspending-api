from django.apps import apps

from usaspending_api.awards.models import Award, TransactionNormalized
from usaspending_api.common.exceptions import InvalidParameterException

loc_dict = {
    'country': 'location_country_code',
    'state': 'state_code',
    'county': 'county_code',
    'district': 'congressional_code',
    'zip': 'zip5',
}


def geocode_filter_locations(scope, values, model):
    """
    Function filter querysets on location table
    scope- place of performance or recipient location mappings
    values- array of location requests
    model- awards or transactions will create queryset for model
    returns queryset
    """
    or_queryset = None
    queryset_init = False

    for v in values:
        fields = v.keys()

        check_location_fields(fields)

        kwargs = {}

        for loc_scope in fields:
            if loc_dict.get(loc_scope) is not None:
                key_str = '{0}__{1}__in'.format(scope, loc_dict.get(loc_scope))
                kwargs[key_str] = get_fields_list(loc_dict.get(loc_scope), v.get(loc_scope))

        model_name = apps.get_model('awards', model)
        qs = model_name.objects.filter(**kwargs)

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


def get_fields_list(scope, field_value):
    """List of values to search for; `field_value`, plus possibley variants on it"""

    if scope not in ['state_code', 'location_country_code', 'zip5']:
        return [str(int(field_value)), field_value, str(float(field_value))]

    return [field_value]
