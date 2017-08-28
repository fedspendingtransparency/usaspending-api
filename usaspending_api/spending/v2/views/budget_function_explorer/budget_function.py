from django.db.models import F, Sum

from usaspending_api.spending.v2.views.budget_function_explorer.budget_subfunction import budget_subfunction


def budget_function(queryset, fiscal_year):
    # Budget Function Queryset
    bf = queryset.annotate(
        budget_function_code=F('treasury_account__budget_function_code'),
        budget_function_name=F('treasury_account__budget_function_title'),
    ).values('budget_function_code', 'budget_function_name').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    function_total = bf.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in function_total.items():
        function_total = value

    # Unpack budget_subfunction results
    budget_sub_function_results = budget_subfunction(queryset, fiscal_year)

    budget_function_results = {
        'count': bf.count(),
        'total': function_total,
        'end_date': fiscal_year,
        'budget_function': bf,
        'budget_sub_function': budget_sub_function_results
    }
    results = [
        budget_function_results
    ]
    return results
