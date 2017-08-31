from django.db.models import F, Sum, CharField, Value


def budget_subfunction(queryset, fiscal_year):
    # Budget Sub Function Queryset
    budget_sub_function = queryset.annotate(
        id=F('treasury_account__budget_function_code'),
        type=Value('budget_subfunction', output_field=CharField()),
        name=F('treasury_account__budget_subfunction_title'),
        code=F('treasury_account__budget_subfunction_code'),
        amount=Sum('obligations_incurred_total_by_award_cpe')
    ).values('id', 'type', 'name', 'code', 'amount').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    budget_sub_function_total = budget_sub_function.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in budget_sub_function_total.items():
        budget_sub_function_total = value

    budget_sub_function_results = {
        'total': budget_sub_function_total,
        'end_date': fiscal_year,
        'results': budget_sub_function
    }
    return budget_sub_function_results
