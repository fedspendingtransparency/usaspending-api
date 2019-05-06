from django.conf import settings

from usaspending_api.awards.models import Award
from usaspending_api.awards.v2.filters.location_filter_geocode import location_error_handling
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping, contract_type_mapping, idv_type_mapping
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.helpers import (
    check_types_and_assign_defaults,
    parse_limit,
    validate_time_periods,
)
from usaspending_api.download.lookups import (
    VALUE_MAPPINGS,
    SHARED_AWARD_FILTER_DEFAULTS,
    YEAR_CONSTRAINT_FILTER_DEFAULTS,
    ROW_CONSTRAINT_FILTER_DEFAULTS,
    ACCOUNT_FILTER_DEFAULTS,
)


def validate_award_request(request_data):
    """Analyze request and raise any formatting errors as Exceptions"""

    _validate_required_parameters(request_data, ['award_levels', 'filters'])
    filters = _validate_filters(request_data)
    award_levels = _validate_award_levels(request_data)

    json_request = {'download_types': award_levels, 'filters': {}}

    # Overriding all other filters if the keyword filter is provided in year-constraint download
    # Make sure this is after checking the award_levels
    constraint_type = request_data.get('constraint_type')
    if constraint_type == 'year' and 'elasticsearch_keyword' in filters:
        json_request['filters'] = {
            'elasticsearch_keyword': filters['elasticsearch_keyword'],
            'award_type_codes': list(award_type_mapping.keys()),
        }
        json_request['limit'] = settings.MAX_DOWNLOAD_LIMIT
        return json_request

    # Set defaults of non-required parameters
    json_request['columns'] = request_data.get('columns', [])
    json_request['file_format'] = request_data.get('file_format', 'csv')

    check_types_and_assign_defaults(filters, json_request['filters'], SHARED_AWARD_FILTER_DEFAULTS)

    json_request['filters']['award_type_codes'] = _validate_award_type_codes(filters)

    _validate_and_update_locations(filters, json_request)

    # Validate time periods
    total_range_count = validate_time_periods(filters, json_request)

    if constraint_type == 'row_count':
        # Validate limit exists and is below MAX_DOWNLOAD_LIMIT
        json_request['limit'] = parse_limit(request_data)

        # Validate row_count-constrained filter types and assign defaults
        check_types_and_assign_defaults(filters, json_request['filters'], ROW_CONSTRAINT_FILTER_DEFAULTS)
    elif constraint_type == 'year':
        # Validate combined total dates within one year (allow for leap years)
        if total_range_count > 366:
            raise InvalidParameterException('Invalid Parameter: time_period total days must be within a year')

        # Validate year-constrained filter types and assign defaults
        check_types_and_assign_defaults(filters, json_request['filters'], YEAR_CONSTRAINT_FILTER_DEFAULTS)
    else:
        raise InvalidParameterException('Invalid parameter: constraint_type must be "row_count" or "year"')

    return json_request


def validate_idv_request(request_data):
    _validate_required_parameters(request_data, ["award_id"])
    award_id, piid = _validate_award_id(request_data)

    return {
        "account_level": "treasury_account",
        "download_types": ["idv_orders", "idv_transaction_history", "idv_federal_account_funding"],
        "file_format": request_data.get("file_format", "csv"),
        "include_file_description": {
            "source": settings.IDV_DOWNLOAD_README_FILE_PATH,
            "destination": "readme.txt"
        },
        "piid": piid,
        "is_for_idv": True,
        "filters": {
            "idv_award_id": award_id,
            "award_type_codes": tuple(set(contract_type_mapping) | set(idv_type_mapping)),
        },
        "limit": parse_limit(request_data),
    }


def validate_account_request(request_data):
    json_request = {'columns': request_data.get('columns', []), 'filters': {}}

    _validate_required_parameters(request_data, ["account_level", "filters"])
    json_request['account_level'] = _validate_account_level(request_data, ["federal_account", "treasury_account"])

    filters = _validate_filters(request_data)

    # Validate required filters
    for required_filter in ["fy", "quarter"]:
        if required_filter not in filters:
            raise InvalidParameterException('Missing one or more required filters: {}'.format(required_filter))
        else:
            try:
                filters[required_filter] = int(filters[required_filter])
            except (TypeError, ValueError):
                raise InvalidParameterException('{} filter not provided as an integer'.format(required_filter))
        json_request['filters'][required_filter] = filters[required_filter]

    # Validate fiscal_quarter
    if json_request['filters']['quarter'] not in [1, 2, 3, 4]:
        raise InvalidParameterException('quarter filter must be a valid fiscal quarter (1, 2, 3, or 4)')

    # Validate submission_type parameters
    valid_submissions = ["account_balances", "object_class_program_activity", "award_financial"]
    submission_type = filters.get('submission_type', None)

    if submission_type not in valid_submissions:
        raise InvalidParameterException('Invalid Parameter: submission_type must be {}'.format(valid_submissions))

    json_request['download_types'] = [filters['submission_type']]

    # Validate the rest of the filters
    check_types_and_assign_defaults(filters, json_request['filters'], ACCOUNT_FILTER_DEFAULTS)

    return json_request


def _validate_award_id(request_data):
    """
    Validate that we were provided a valid award_id and, in the process, convert
    generated_unique_award_id to an internal, surrogate, integer award id.

    Returns the surrogate award id and piid.
    """
    award_id = request_data.get('award_id')
    if award_id is None:
        raise InvalidParameterException("Award id must be provided and may not be null")
    award_id_type = type(award_id)
    if award_id_type not in (str, int):
        raise InvalidParameterException("Award id must be either a string or an integer")
    if award_id_type is int or award_id.isdigit():
        award = Award.objects.filter(
            id=int(award_id), type__startswith="IDV").values_list("id", "piid").first()
    else:
        award = Award.objects.filter(
            generated_unique_award_id=award_id, type__startswith="IDV").values_list("id", "piid").first()
    if not award:
        raise InvalidParameterException("Unable to find an IDV matching the provided award id")
    return award


def _validate_account_level(request_data, valid_account_levels):
    account_level = request_data.get('account_level')
    if account_level not in valid_account_levels:
        raise InvalidParameterException("Invalid Parameter: account_level must be {}".format(valid_account_levels))
    return account_level


def _validate_award_levels(request_data):
    award_levels = request_data.get('award_levels')
    if not isinstance(award_levels, list):
        raise InvalidParameterException('Award levels parameter not provided as a list')
    elif len(award_levels) == 0:
        raise InvalidParameterException('At least one award level is required.')
    for award_level in award_levels:
        if award_level not in VALUE_MAPPINGS:
            raise InvalidParameterException('Invalid award_level: {}'.format(award_level))
    return award_levels


def _validate_award_type_codes(filters):
    award_type_codes = filters.get('award_type_codes')
    if not award_type_codes or len(award_type_codes) < 1:
        award_type_codes = list(award_type_mapping)
    for award_type_code in award_type_codes:
        if award_type_code not in award_type_mapping:
            raise InvalidParameterException('Invalid award_type: {}'.format(award_type_code))
    return award_type_codes


def _validate_filters(request_data):
    filters = request_data.get('filters')
    if not isinstance(filters, dict):
        raise InvalidParameterException('Filters parameter not provided as a dict')
    elif len(filters) == 0:
        raise InvalidParameterException('At least one filter is required.')
    return filters


def _validate_and_update_locations(filters, json_request):
    if 'filters' not in json_request:
        json_request['filters'] = {}
    for location_filter in ['place_of_performance_locations', 'recipient_locations']:
        if filters.get(location_filter):
            for location_dict in filters[location_filter]:
                if not isinstance(location_dict, dict):
                    raise InvalidParameterException('Location is not a dictionary: {}'.format(location_dict))
                location_error_handling(location_dict.keys())
            json_request['filters'][location_filter] = filters[location_filter]


def _validate_required_parameters(request_data, required_parameters):
    for required_param in required_parameters:
        if required_param not in request_data:
            raise InvalidParameterException(
                'Missing one or more required body parameters: {}'.format(required_param)
            )
