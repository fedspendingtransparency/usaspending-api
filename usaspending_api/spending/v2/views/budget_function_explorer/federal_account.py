from django.db.models import F, Sum

from usaspending_api.spending.v2.views.budget_function_explorer.object_class import object_class_budget


def federal_account_budget(queryset, fiscal_year):
    # Federal Account Queryset
    federal_accounts = queryset.annotate(
        sub_function_code=F('treasury_account__budget_subfunction_code'),
        federal_account_name=F('treasury_account__federal_account__account_title'),
        main_account_code=F('treasury_account__federal_account__main_account_code')
    ).values(
        'sub_function_code', 'federal_account_name', 'main_account_code').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    federal_accounts_total = federal_accounts.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in federal_accounts_total.items():
        federal_accounts_total = value

    # Unpack object class federal account results
    object_classes_results = object_class_budget(queryset, fiscal_year)

    federal_accounts_results = {
        'count': federal_accounts.count(),
        'total': federal_accounts_total,
        'end_date': fiscal_year,
        'federal_account': federal_accounts,
        'object_class': object_classes_results
    }
    results = [
        federal_accounts_results
    ]
    return results
