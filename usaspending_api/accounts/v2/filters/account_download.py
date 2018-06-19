import datetime

from django.db.models import Case, CharField, DecimalField, OuterRef, Subquery, Sum, Value, When
from django.db.models.functions import Coalesce, Concat

from usaspending_api.accounts.helpers import start_and_end_dates_from_fyq
from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.v2.download_column_historical_lookups import query_paths
from usaspending_api.references.models import ToptierAgency


def account_download_filter(account_type, download_table, filters, account_level='treasury_account'):
    query_filters = {}
    tas_id = 'treasury_account_identifier' if account_type == 'account_balances' else 'treasury_account'

    # Filter by Agency, if provided
    if filters.get('agency', False) and filters['agency'] != 'all':
        agency = ToptierAgency.objects.filter(toptier_agency_id=filters['agency']).first()
        if agency:
            query_filters['{}__agency_id'.format(tas_id)] = agency.cgac_code
        else:
            raise InvalidParameterException('agency with that ID does not exist')

    # TODO: Filter by Federal Account, if provided
    # if filters.get('federal_account', False):
    #     federal_account_obj = FederalAccount.objects.filter(id=filters['federal_account']).first()
    #     if federal_account_obj:
    #         query_filters['{}__federal_account__id'.format(tas_id)] = filters['federal_account']
    #     else:
    #         raise InvalidParameterException('agency with that ID does not exist')

    # Filter by Fiscal Year and Quarter
    reporting_period_start, reporting_period_end, start_date, end_date = retrieve_fyq_filters(account_type, filters)
    query_filters[reporting_period_start] = start_date
    query_filters[reporting_period_end] = end_date

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

    # Derive recipient_parent_name and transaction_obligated_amount_ for award_financial downloads
    if account_type == 'award_financial':
        derived_fields['recipient_parent_name'] = Case(
            When(award__latest_transaction__type__in=list(contract_type_mapping.keys()),
                 then='award__latest_transaction__contract_data__ultimate_parent_legal_enti'),
            default='award__latest_transaction__assistance_data__ultimate_parent_legal_enti',
            output_field=CharField())

        # Account for NaN bug in award_financial data
        # TODO: Fix the data and get rid of this code
        derived_fields['transaction_obligated_amount_'] = Case(
            When(transaction_obligated_amount=Value('NaN')), then=Value(0.00), default='transaction_obligated_amount',
            output_field=DecimalField())

    return queryset.annotate(**derived_fields)


def generate_federal_account_query(queryset, account_type, tas_id):
    """ Group by federal account (and budget function/subfunction) and SUM all other fields """
    # Derive the federal_account_symbol and agency_name
    agency_name_subquery = ToptierAgency.objects.filter(
        cgac_code=OuterRef('{}__federal_account__agency_identifier'.format(tas_id)))
    derived_fields = {
        # federal_account_symbol: fed_acct_AID-fed_acct_MAC
        'federal_account_symbol': Concat('{}__federal_account__agency_identifier'.format(tas_id), Value('-'),
                                         '{}__federal_account__main_account_code'.format(tas_id)),

        # agency_name: name of the ToptierAgency with CGAC matching federal_account__agency_identifier
        'agency_name': Subquery(agency_name_subquery.values('name')[:1])
    }

    # Derive recipient_parent_name for award_financial downloads
    if account_type == 'award_financial':
        derived_fields['recipient_parent_name'] = Case(
            When(award__latest_transaction__type__in=list(contract_type_mapping.keys()),
                 then='award__latest_transaction__contract_data__ultimate_parent_legal_enti'),
            default='award__latest_transaction__assistance_data__ultimate_parent_legal_enti',
            output_field=CharField())

    queryset = queryset.annotate(**derived_fields)

    # List of all columns that may appear in A, B, or C files that can be summed
    all_summed_cols = [
        'budget_authority_unobligated_balance_brought_forward', 'adjustments_to_unobligated_balance_brought_forward',
        'budget_authority_appropriated_amount', 'borrowing_authority_amount', 'contract_authority_amount',
        'spending_authority_from_offsetting_collections_amount', 'total_other_budgetary_resources_amount',
        'total_budgetary_resources', 'obligations_incurred', 'deobligations_or_recoveries_or_refunds_from_prior_year',
        'unobligated_balance', 'gross_outlay_amount', 'status_of_budgetary_resources_total',
        'transaction_obligated_amount_']  # TODO: remove trailing  "_" from transaction_obligated_amount_ after data fix

    # Group by all columns within the file that can't be summed
    fed_acct_values_dict = query_paths[account_type]['federal_account']
    grouped_cols = [fed_acct_values_dict[val] for val in fed_acct_values_dict if val not in all_summed_cols]
    queryset = queryset.values(*grouped_cols)

    # Sum all fields from all_summed_cols that appear in this file
    values_dict = query_paths[account_type]
    summed_cols = {val: Sum(values_dict['treasury_account'].get(val, None)) for val in values_dict['federal_account']
                   if val in all_summed_cols}

    # Account for NaN bug in award_financial data
    # TODO: Fix the data and get rid of this code
    if account_type == 'award_financial':
        summed_cols['transaction_obligated_amount_'] = Sum(
            Coalesce(Case(When(transaction_obligated_amount=Value('NaN')), then=Value(0.00),
                          default='transaction_obligated_amount', output_field=DecimalField()), 0))

    return queryset.annotate(**summed_cols)


def retrieve_fyq_filters(account_type, filters):
    """ Apply a filter by Fiscal Year and Quarter """
    if filters.get('fy', False) and filters.get('quarter', False):
        start_date, end_date = start_and_end_dates_from_fyq(filters['fy'], filters['quarter'])

        reporting_period_start = 'reporting_period_start'
        reporting_period_end = 'reporting_period_end'

        # C files need all data, up to and including the FYQ in the filter
        if account_type == 'award_financial':
            reporting_period_start = '{}__gte'.format(reporting_period_start)
            reporting_period_end = '{}__lte'.format(reporting_period_end)
            start_date = datetime.date(filters['fy']-1, 10, 1)
    else:
        raise InvalidParameterException('fy and quarter are required parameters')

    return reporting_period_start, reporting_period_end, start_date, end_date
