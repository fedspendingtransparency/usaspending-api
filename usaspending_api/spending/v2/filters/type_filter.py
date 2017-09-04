from datetime import datetime

from django.db.models import Sum

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.spending.v2.filters.fy_filter import fy_filter
from usaspending_api.spending.v2.filters.spending_filter import spending_filter
from usaspending_api.spending.v2.views.explorer import Explorer


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
        alt_set, queryset = spending_filter(alt_set, queryset, filters, explorer)

        # Annotate and get explorer type data
        exp = Explorer(alt_set, queryset)

        if explorer == 'recipient':
            total, alt_set = exp.recipient()
        if explorer == 'award':
            total, alt_set = exp.award()
        if explorer == 'award_category':
            total, alt_set = exp.award_category()
        if explorer == 'agency_sub':
            total, alt_set = exp.awarding_sub_tier_agency()

        results = {
            'total': total,
            'end_date': fiscal_date,
            'results': alt_set
        }

    else:
        # Apply filters to queryset and alt_set
        alt_set, queryset = spending_filter(alt_set, queryset, filters, explorer)

        # Annotate and get explorer type data
        exp = Explorer(alt_set, queryset)

        if explorer == 'budget_function':
            total, queryset = exp.budget_function()
        if explorer == 'budget_subfunction':
            total, queryset = exp.budget_subfunction()
        if explorer == 'federal_account':
            total, queryset = exp.federal_account()
        if explorer == 'program_activity':
            total, queryset = exp.program_activity()
        if explorer == 'object_class':
            total, queryset = exp.object_class()
        if explorer == 'agency':
            total, queryset = exp.agency()
        if explorer == 'agency_top':
            total, queryset = exp.awarding_top_tier_agency()

        results = {
            'total': total,
            'end_date': fiscal_date,
            'results': queryset
        }

    return results
