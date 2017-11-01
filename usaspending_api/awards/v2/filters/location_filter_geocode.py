from usaspending_api.awards.models import Award, TransactionNormalized
from usaspending_api.common.exceptions import InvalidParameterException

import logging
logger = logging.getLogger(__name__)


def geocode_filter_locations(scope, values, model):
    """
    Function to return filter querysets for location table
    :param scope: place of performance or recipient location mappings
    :param values: array of location requests
    :param model: awards or transactions will create queryset for model
    :return: queryset
    """
    loc_dict = {
        'country': 'location_country_code',
        'state': 'state_code',
        'county': 'county_code',
        'district': 'congressional_code'
    }
    or_queryset = None

    for v in values:
        fields = v.keys()

        # Request must have country, and can only have 3 fields,
        # and must have state if 3 fields (county or district)
        if len(v) == 0 or 'country' not in fields or len(v) > 3 \
                or ('state' not in fields and len(v) == 3):
            raise InvalidParameterException(
                'Invalid filter: recipient has incorrect object.'
            )

        kwargs = {'{0}__{1}'.format(scope, loc_dict.get(loc_scope)): v.get(loc_scope)
                  for loc_scope in fields
                  if loc_dict.get(loc_scope) is not None}

        # If lengths don't match one of the request fields
        # is not in the loc_dict
        if len(kwargs) != len(v):
            raise InvalidParameterException(
                'Invalid filter: recipient has incorrect object options ' +
                'include: county, state, county, district')

        if model == 'award':
            qs = Award.objects.filter(**kwargs)
        elif model == 'transaction':
            qs = TransactionNormalized.objects.filter(**kwargs)
        else:
            logger.error(
                'Incorrect model for filter locations function ' +
                'options are: award or transaction'
            )

        if or_queryset is not None:
            or_queryset |= qs
        else:
            or_queryset = qs

    return or_queryset
