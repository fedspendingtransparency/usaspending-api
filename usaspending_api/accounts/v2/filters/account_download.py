from django.db.models import Case, CharField, OuterRef, Subquery, Sum, Value, When
from django.db.models.functions import Concat

from usaspending_api.accounts.helpers import start_and_end_dates_from_fyq
from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.v2.download_column_historical_lookups import query_paths
from usaspending_api.references.models import ToptierAgency


def account_download_filter(account_type, download_table, filters, account_level='treasury_account'):
    query_filters = {}
    tas_id = 'treasury_account_identifier' if account_type == 'account_balances' else 'treasury_account'

    # Filter by agency if necessary
    if filters.get('agency', False) and filters['agency'] != 'all':
        agency = ToptierAgency.objects.filter(toptier_agency_id=filters['agency']).first()
        if agency:
            query_filters['{}__agency_id'.format(tas_id)] = agency.cgac_code
        else:
            raise InvalidParameterException('agency with that ID does not exist')

    # TODO: Filter by federal account
    # federal_account = filters.get('federal_account', False)
    # if federal_account:
    #     query_filters['{}__federal_account__federal_account_code'.format(tas_id)] = federal_account

    # Filter by Fiscal Year and Quarter
    if filters.get('fy', False) and filters.get('quarter', False):
        start_date, end_date = start_and_end_dates_from_fyq(filters['fy'], filters['quarter'])
        query_filters['reporting_period_start'] = start_date
        query_filters['reporting_period_end'] = end_date
    else:
        raise InvalidParameterException('fy and quarter are required parameters')

    # Create the base queryset
    queryset = download_table.objects

    # Make derivations based on the account level
    if account_level == 'treasury_account':
        queryset = generate_treasury_account_query(queryset, account_type, tas_id)

    elif account_level == 'federal_account':
        queryset = generate_federal_account_query(queryset, account_type, tas_id)

    else:
        raise InvalidParameterException('Invalid Parameter: account_level must be either "federal_account" or '
                                        '"treasury_account"')

    # Apply filter and return
    return queryset.filter(**query_filters)


def generate_treasury_account_query(queryset, account_type, tas_id):
    """ Derive necessary fields for a treasury account-grouped query """
    # Derive treasury_account_symbol, allocation_transfer_agency_name, agency_name, and federal_account_symbol
    # for all account types
    ata_subquery = ToptierAgency.objects.filter(cgac_code=OuterRef('{}__allocation_transfer_agency_id'.format(tas_id)))
    agency_name_subquery = ToptierAgency.objects.filter(cgac_code=OuterRef('{}__agency_id'.format(tas_id)))

    derived_fields = {
        # treasury_account_symbol: AID-BPOA/EPOA-MAC-SAC or AID-"X"-MAC-SAC
        'treasury_account_symbol': Concat(
            '{}__agency_id'.format(tas_id),
            Value('-'),
            Case(When(**{'{}__availability_type_code'.format(tas_id): 'X', 'then': Value('X')}),
                 default=Concat('{}__beginning_period_of_availability'.format(tas_id), Value('/'),
                                '{}__ending_period_of_availability'.format(tas_id)),
                 output_field=CharField()),
            Value('-'),
            '{}__main_account_code'.format(tas_id),
            Value('-'),
            '{}__sub_account_code'.format(tas_id),
            output_field=CharField()),

        # allocation_transfer_agency_name: name of the ToptierAgency with CGAC matching allocation_transfer_agency_id
        'allocation_transfer_agency_name': Subquery(ata_subquery.values('name')[:1]),

        # agency_name: name of the ToptierAgency with CGAC matching agency_id
        'agency_name': Subquery(agency_name_subquery.values('name')[:1]),

        # federal_account_symbol: fed_acct_AID-fed_acct_MAC
        'federal_account_symbol': Concat('{}__federal_account__agency_identifier'.format(tas_id), Value('-'),
                                         '{}__federal_account__main_account_code'.format(tas_id))
    }

    # Derive recipient_parent_name for award_financial downloads
    if account_type == 'award_financial':
        derived_fields['recipient_parent_name'] = Case(
            When(award__latest_transaction__type__in=list(contract_type_mapping.keys()),
                 then='award__latest_transaction__contract_data__ultimate_parent_legal_enti'),
            default='award__latest_transaction__assistance_data__ultimate_parent_legal_enti',
            output_field=CharField())

    return queryset.annotate(**derived_fields)


def generate_federal_account_query(queryset, account_type, tas_id):
    """ Group by federal account (and budget function/subfunction) and SUM all other fields """
    # Derive the federal_account_symbol
    queryset = queryset.annotate(
        federal_account_symbol=Concat('{}__federal_account__agency_identifier'.format(tas_id), Value('-'),
                                      '{}__federal_account__main_account_code'.format(tas_id)))

    # Group by federal_account_symbol, budget_function, budget_subfunction
    group_vals = ['federal_account_symbol', '{}__federal_account__account_title'.format(tas_id),
                  '{}__budget_function_title'.format(tas_id), '{}__budget_subfunction_title'.format(tas_id)]
    queryset = queryset.values(*group_vals)

    # Sum all fields excluding the ones we're grouping by
    q_path = query_paths[account_type]
    summed_cols = {i: Sum(q_path['treasury_account'][i]) for i in q_path['federal_account']
                   if q_path['treasury_account'][i] not in group_vals}

    return queryset.annotate(**summed_cols)
