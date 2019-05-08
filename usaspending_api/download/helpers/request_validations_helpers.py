import logging

from datetime import datetime
from django.conf import settings
from rest_framework.exceptions import ParseError

from usaspending_api.common.exceptions import InvalidParameterException

logger = logging.getLogger('console')


def check_types_and_assign_defaults(old_dict, new_dict, defaults_dict):
    """Validates the types of the old_dict, and assigns them to the new_dict"""
    for field in defaults_dict.keys():
        # Set the new value to the old value, or the default if it doesn't exist
        new_dict[field] = old_dict.get(field, defaults_dict[field])

        # Validate the field's data type
        if not isinstance(new_dict[field], type(defaults_dict[field])):
            type_name = type(defaults_dict[field]).__name__
            raise InvalidParameterException('{} parameter not provided as a {}'.format(field, type_name))

        # Remove empty filters
        if new_dict[field] == defaults_dict[field]:
            del(new_dict[field])


def parse_limit(json_request):
    """Validates the limit from a json_request
    Returns the limit as an int"""
    limit = json_request.get('limit')
    if limit:
        try:
            limit = int(json_request['limit'])
        except (ValueError, TypeError):
            raise ParseError('Parameter "limit" must be int; {} given'.format(limit))
        if limit > settings.MAX_DOWNLOAD_LIMIT:
            msg = 'Requested limit {} beyond max supported ({})'
            raise ParseError(msg.format(limit, settings.MAX_DOWNLOAD_LIMIT))
    else:
        limit = settings.MAX_DOWNLOAD_LIMIT
    return limit   # None is a workable slice argument


def validate_time_periods(filters, new_request):
    """Validates passed time_period filters, or assigns the default values.
    Returns the number of days selected by the user"""
    default_date_values = {
        'start_date': '1000-01-01',
        'end_date': datetime.strftime(datetime.utcnow(), '%Y-%m-%d'),
        'date_type': 'action_date'
    }

    # Enforcing that time_period always has content
    if len(filters.get('time_period', [])) == 0:
        filters['time_period'] = [default_date_values]
    new_request['filters']['time_period'] = filters['time_period']

    total_range_count = 0
    for date_range in new_request['filters']['time_period']:
        # Empty strings, Nones, or missing keys should be replaced with the default values
        for key in default_date_values:
            date_range[key] = date_range.get(key, default_date_values[key])
            if date_range[key] == '':
                date_range[key] = default_date_values[key]

        # Validate date values
        try:
            d1 = datetime.strptime(date_range['start_date'], "%Y-%m-%d")
            d2 = datetime.strptime(date_range['end_date'], "%Y-%m-%d")
        except ValueError:
            raise InvalidParameterException('Date Ranges must be in the format YYYY-MM-DD.')

        # Add to total_range_count for year-constraint validations
        total_range_count += (d2 - d1).days

        # Validate and derive date type
        if date_range['date_type'] not in ['action_date', 'last_modified_date']:
            raise InvalidParameterException(
                'Invalid parameter within time_period\'s date_type: {}'.format(date_range['date_type']))

    return total_range_count
