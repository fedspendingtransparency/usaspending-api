from django.db.models import F, Sum

from usaspending_api.spending.v2.views.budget_function_explorer.federal_account import federal_account_budget


def budget_subfunction(queryset, fiscal_year):
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

    # Unpack federal account object class results
    federal_accounts_results = federal_account_budget(queryset, fiscal_year)

    budget_sub_function_results = {
        'count': budget_sub_function.count(),
        'total': budget_sub_function_total,
        'end_date': fiscal_year,
        'budget_sub_function': budget_sub_function,
        'federal_account': federal_accounts_results
    }
    results = [
        budget_sub_function_results
    ]
    return results
