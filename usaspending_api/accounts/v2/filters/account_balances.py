import datetime
import logging

from usaspending_api.accounts.helpers import start_and_end_dates_from_fyq
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.common.exceptions import InvalidParameterException

logger = logging.getLogger(__name__)


def account_balances_filter(filters):
    query_filters = {}

    # Filter by agency if necessary
    if filters.get('agency', False) and filters['agency'] != 'all':
        query_filters['treasury_account_identifier__agency_id'] = filters['agency']

    # TODO: Filter by federal account
    # if filters.get('federal_account', False):
    #     query_filters['treasury_account_identifier__federal_account__federal_account_code'] = filters['federal_account']

    # Filter by Fiscal Year and Quarter
    if filters.get('fiscal_year', False) and filters.get('fiscal_quarter', False):
        start_date, end_date = start_and_end_dates_from_fyq(filters['fiscal_year'], filters['fiscal_quarter'])
        query_filters['reporting_period_start'] = start_date
        query_filters['reporting_period_end'] = end_date
    else:
        raise InvalidParameterException('fiscal_year and fiscal_quarter are required parameters')

    return AppropriationAccountBalances.objects.filter(**query_filters)
