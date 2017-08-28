from django.db.models import F, Sum
from datetime import datetime

from usaspending_api.explorers.v2.filters.fy_filter import fy_filter
from usaspending_api.explorers.v2.views.federal_account import federal_account_budget


def budget_subfunction(queryset):
    fiscal_year = fy_filter(datetime.now().date())
    # Budget Sub Function Queryset
    budget_sub_function = queryset.annotate(
        budget_function_code=F('treasury_account__budget_function_code'),
        sub_function_code=F('treasury_account__budget_subfunction_code'),
        sub_function_name=F('treasury_account__budget_subfunction_title')
    ).values('budget_function_code', 'sub_function_code', 'sub_function_name').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    budget_sub_function_total = budget_sub_function.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in budget_sub_function_total.items():
        budget_sub_function_total = value

    budget_sub_function_results = {
        'count': budget_sub_function.count(),
        'total': budget_sub_function_total,
        'end_date': fiscal_year,
        'budget_sub_function': budget_sub_function,
    }

    # Unpack federal account object class results
    federal_accounts_results, object_classes_results, recipients_results,\
        award_category_results, awards_results = federal_account_budget(queryset)

    results = [
        budget_sub_function_results,
        federal_accounts_results,
        object_classes_results,
        recipients_results,
        award_category_results,
        awards_results
    ]
    return results
