from django.db.models import F, Sum, CharField, Value


def budget_subfunction(queryset, fiscal_date):
    # Budget Sub Function Queryset
    queryset = queryset.annotate(
        id=F('treasury_account__budget_subfunction_code'),
        type=Value('budget_subfunction', output_field=CharField()),
        name=F('treasury_account__budget_subfunction_title'),
        code=F('treasury_account__budget_subfunction_code'),
        amount=Sum('obligations_incurred_by_program_object_class_cpe')
    ).values('id', 'type', 'name', 'code', 'amount').annotate(
        total=Sum('obligations_incurred_by_program_object_class_cpe')).order_by('-total')

    total = queryset.aggregate(Sum('obligations_incurred_by_program_object_class_cpe'))
    for key, value in total.items():
        total = value

    budget_sub_function_results = {
        'total': total,
        'end_date': fiscal_date,
        'results': queryset
    }
    return total, fiscal_date, queryset
