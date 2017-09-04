from datetime import datetime
from decimal import Decimal
from itertools import chain

from django.db.models import Sum

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.spending.v2.filters.fy_filter import fy_filter
from usaspending_api.spending.v2.filters.spending_filter import spending_filter
from usaspending_api.spending.v2.views.agency import agency, awarding_top_tier_agency, \
    awarding_sub_tier_agency
from usaspending_api.spending.v2.views.award import award, award_category
from usaspending_api.spending.v2.views.budget_function import budget_function
from usaspending_api.spending.v2.views.budget_subfunction import budget_subfunction
from usaspending_api.spending.v2.views.federal_account import federal_account
from usaspending_api.spending.v2.views.object_class import object_class
from usaspending_api.spending.v2.views.program_activity import program_activity
from usaspending_api.spending.v2.views.recipient import recipient


def type_filter(explorer, filters):
    total = None
    fiscal_quarter = None
    fiscal_date = None

    explorers = ['budget_function', 'budget_subfunction', 'federal_account', 'program_activity', 'object_class',
                 'recipient', 'award', 'award_category', 'agency', 'agency_top', 'agency_sub']

    # Validate explorer type
    if explorer is None:
        raise InvalidParameterException('Missing one or more required request parameters: type')

    elif explorer not in explorers:
        raise InvalidParameterException(
            'Explorer does not have a valid value. '
            'Valid Explorers: budget_function, budget_subfunction, federal_account, '
            'program_activity, object_class, recipient, award, award_category agency, agency_top, agency_sub')

    # Get fiscal_date and fiscal_quarter
    for key, value in filters.items():
        if key == 'fy':
            if value is not None:
                fiscal_date, fiscal_quarter = fy_filter(value, datetime.now().date())
            else:
                raise InvalidParameterException('Incorrect or Missing fiscal year: YYYY')

    # Recipient, Award Queryset
    alt_set = FinancialAccountsByAwards.objects.all().exclude(
            transaction_obligated_amount__isnull=True).filter(
            submission__reporting_fiscal_quarter=fiscal_quarter).annotate(
            amount=Sum('transaction_obligated_amount'))

    # Base Queryset
    queryset = FinancialAccountsByProgramActivityObjectClass.objects.all().exclude(
            obligations_incurred_by_program_object_class_cpe__isnull=True).filter(
            submission__reporting_fiscal_quarter=fiscal_quarter).annotate(
            amount=Sum('obligations_incurred_by_program_object_class_cpe'))

    if explorer == 'recipient' or explorer == 'award' or explorer == 'award_category' or explorer == 'agency_sub':

        # Apply filters to queryset and alt_set
        alt_set, queryset = spending_filter(queryset, alt_set, filters, explorer)

        # Retrieve explorer type data
        if explorer == 'recipient':
            total, fiscal_date, alt_set = recipient(alt_set, fiscal_date)
        if explorer == 'award':
            total, fiscal_date, alt_set = award(alt_set, fiscal_date)
        if explorer == 'award_category':
            total, fiscal_date, queryset = award_category(alt_set, fiscal_date)
        if explorer == 'agency_sub':
            total, fiscal_date, alt_set = awarding_sub_tier_agency(alt_set, fiscal_date)

        results = {
            'total': total,
            'end_date': fiscal_date,
            'results': alt_set
        }

    else:
        # Apply filters to queryset and alt_set
        queryset, alt_set = spending_filter(queryset, alt_set, filters, explorer)
        print(queryset)

        # Retrieve explorer type data
        if explorer == 'budget_function':
            total, fiscal_date, queryset = budget_function(queryset, fiscal_date)
        if explorer == 'budget_subfunction':
            total, fiscal_date, queryset = budget_subfunction(queryset, fiscal_date)
        if explorer == 'federal_account':
            total, fiscal_date, queryset = federal_account(queryset, fiscal_date)
        if explorer == 'program_activity':
            total, fiscal_date, queryset = program_activity(queryset, fiscal_date)
        if explorer == 'object_class':
            total, fiscal_date, queryset = object_class(queryset, fiscal_date)
        if explorer == 'agency':
            total, fiscal_date, queryset = agency(queryset, fiscal_date)
        if explorer == 'agency_top':
            total, fiscal_date, queryset = awarding_top_tier_agency(queryset, fiscal_date)

        results = {
            'total': total,
            'end_date': fiscal_date,
            'results': queryset
        }

    return results
