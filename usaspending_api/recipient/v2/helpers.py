import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta

from django.conf import settings
from usaspending_api.common.exceptions import InvalidParameterException

logger = logging.getLogger(__name__)


def validate_year(year=None):
    if year and not (year.isdigit() or year in ['all', 'latest']):
        raise InvalidParameterException('Invalid year: {}.'.format(year))
    return year


def reshape_filters(duns_search_texts=[], duns_hashes=[], state_code=None, year=None, award_type_codes=None):
    # recreate filters for spending over time/category
    filters = {}

    if duns_search_texts:
        filters['recipient_search_text'] = duns_search_texts

    if duns_hashes:
        filters['internal_recipient_ids'] = duns_hashes

    if state_code:
        filters['place_of_performance_locations'] = [{'country': 'USA', 'state': state_code}]

    if year:
        today = datetime.now()
        if year and year.isdigit():
            time_period = [{
                'start_date': '{}-10-01'.format(int(year) - 1),
                'end_date': '{}-09-30'.format(year)
            }]
        elif year == 'all':
            time_period = [{
                'start_date': settings.API_SEARCH_MIN_DATE,
                'end_date': datetime.strftime(today, '%Y-%m-%d')
            }]
        else:
            last_year = today - relativedelta(years=1)
            time_period = [{
                'start_date': datetime.strftime(last_year, '%Y-%m-%d'),
                'end_date': datetime.strftime(today, '%Y-%m-%d')
            }]
        filters['time_period'] = time_period

    if award_type_codes:
        filters['award_type_codes'] = award_type_codes

    return filters
