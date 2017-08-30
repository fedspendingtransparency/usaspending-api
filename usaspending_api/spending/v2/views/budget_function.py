from django.db.models import F, Sum, Value, CharField


def budget_function(queryset, fiscal_year):
    # Budget Function Queryset
    bf = queryset.annotate(
        id=F('financial_accounts_by_awards_id'),
        type=Value('budget_function', output_field=CharField()),
        name=F('treasury_account__budget_function_title'),
        code=F('treasury_account__budget_function_code'),
        amount=F('obligations_incurred_total_by_award_cpe')
    ).values('id', 'type', 'name', 'code', 'amount').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    function_total = bf.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in function_total.items():
        function_total = value

    budget_function_results = {
        'total': function_total,
        'end_date': fiscal_year,
        'results': bf
    }
    return budget_function_results
