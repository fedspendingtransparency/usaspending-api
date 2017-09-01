from django.db.models import F, Sum, CharField, Value


def federal_account(queryset, fiscal_date):
    # Federal Account Queryset
    queryset = queryset.annotate(
        id=F('treasury_account__federal_account__main_account_code'),
        type=Value('federal_account', output_field=CharField()),
        name=F('treasury_account__federal_account__account_title'),
        code=F('treasury_account__federal_account__main_account_code'),
        amount=Sum('obligations_incurred_by_program_object_class_cpe')
    ).values(
        'id', 'type', 'name', 'code', 'amount').annotate(
        total=Sum('obligations_incurred_by_program_object_class_cpe')).order_by('-total')

    total = queryset.aggregate(Sum('obligations_incurred_by_program_object_class_cpe'))
    for key, value in total.items():
        total = value

    federal_accounts_results = {
        'total': total,
        'end_date': fiscal_date,
        'results': queryset
    }
    return total, fiscal_date, queryset
