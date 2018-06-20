import logging

from usaspending_api.accounts.helpers import start_and_end_dates_from_fyq
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.accounts.v2.filters.account_download_derivations import base_treasury_account_derivations
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import ToptierAgency

logger = logging.getLogger(__name__)


def account_balances_filter(filters):
    query_filters = {}

    # Filter by agency if necessary
    if filters.get('agency', False) and filters['agency'] != 'all':
        agency = ToptierAgency.objects.filter(toptier_agency_id=filters['agency']).first()
        if agency:
            query_filters['treasury_account_identifier__agency_id'] = agency.cgac_code
        else:
            raise InvalidParameterException('agency with that ID does not exist')

    # TODO: Filter by federal account
    # federal_account = filters.get('federal_account', False)
    # if federal_account:
    #     query_filters['treasury_account_identifier__federal_account__federal_account_code'] = federal_account

    # Filter by Fiscal Year and Quarter
    if filters.get('fy', False) and filters.get('quarter', False):
        start_date, end_date = start_and_end_dates_from_fyq(filters['fy'], filters['quarter'])
        query_filters['reporting_period_start'] = start_date
        query_filters['reporting_period_end'] = end_date
    else:
        raise InvalidParameterException('fy and quarter are required parameters')

    # Retrieve base Account Download derived fields
    derived_fields = base_treasury_account_derivations('treasury_account_identifier')

    return AppropriationAccountBalances.objects.annotate(**derived_fields).filter(**query_filters)
