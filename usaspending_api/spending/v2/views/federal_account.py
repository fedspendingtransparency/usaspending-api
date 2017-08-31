from django.db.models import F, Sum, CharField, Value


def federal_account_budget(queryset, fiscal_year):
    # Federal Account Queryset
    federal_accounts = queryset.annotate(
        id=F('treasury_account__budget_subfunction_code'),
        type=Value('federal_account', output_field=CharField()),
        name=F('treasury_account__federal_account__account_title'),
        code=F('treasury_account__federal_account__federal_account_code'),
        amount=Sum('obligations_incurred_by_program_object_class_cpe')
    ).values(
        'id', 'type', 'name', 'code', 'amount').annotate(
        total=Sum('obligations_incurred_by_program_object_class_cpe')).order_by('-total')

    federal_accounts_total = federal_accounts.aggregate(Sum('obligations_incurred_by_program_object_class_cpe'))
    for key, value in federal_accounts_total.items():
        federal_accounts_total = value

    federal_accounts_results = {
        'total': federal_accounts_total,
        'end_date': fiscal_year,
        'results': federal_accounts
    }
    return federal_accounts_results
