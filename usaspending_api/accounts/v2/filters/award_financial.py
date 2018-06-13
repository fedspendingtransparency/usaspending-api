import datetime
import logging

from django.db.models import Case, CharField, When

from usaspending_api.accounts.helpers import start_and_end_dates_from_fyq
from usaspending_api.accounts.v2.filters.account_download_derivations import base_treasury_account_derivations
from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.references.models import ToptierAgency

logger = logging.getLogger(__name__)


def award_financial_filter(filters):
    query_filters = {}

    # Filter by agency if necessary
    if filters.get('agency', False) and filters['agency'] != 'all':
        agency = ToptierAgency.objects.filter(toptier_agency_id=filters['agency']).first()
        if agency:
            query_filters['treasury_account__agency_id'] = agency.cgac_code
        else:
            raise InvalidParameterException('agency with that ID does not exist')

    # TODO: Filter by federal account
    # federal_account = filters.get('federal_account', False)
    # if federal_account:
    #     query_filters['treasury_account__federal_account__federal_account_code'] = federal_account

    # Filter by Fiscal Year and Quarter
    if filters.get('fy', False) and filters.get('quarter', False):
        # For C files, we want all the data from Q1 through the quarter given in the filter
        filter_dates = start_and_end_dates_from_fyq(filters['fy'], filters['quarter'])
        query_filters['reporting_period_start__gte'] = datetime.date(filters['fy']-1, 10, 1)
        query_filters['reporting_period_end__lte'] = filter_dates[1]
    else:
        raise InvalidParameterException('fy and quarter are required parameters')

    # Retrieve base Account Download derived fields and add recipient_parent_name
    derived_fields = base_treasury_account_derivations('treasury_account')
    derived_fields['recipient_parent_name'] = Case(
        When(award__latest_transaction__type__in=list(contract_type_mapping.keys()),
             then='award__latest_transaction__contract_data__ultimate_parent_legal_enti'),
        default='award__latest_transaction__assistance_data__ultimate_parent_legal_enti',
        output_field=CharField())

    return FinancialAccountsByAwards.objects.annotate(**derived_fields).filter(**query_filters)
