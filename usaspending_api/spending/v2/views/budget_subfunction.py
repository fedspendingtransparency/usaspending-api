from django.db.models import F, Sum, CharField, Value

from usaspending_api.spending.v2.views.federal_account import federal_account_budget


def budget_subfunction(queryset, fiscal_year):
    # Budget Sub Function Queryset
    budget_sub_function = queryset.annotate(
        id=F('financial_accounts_by_awards_id'),
        type=Value('budget_subfunction', output_field=CharField()),
        name=F('treasury_account__budget_subfunction_title'),
        code=F('treasury_account__budget_subfunction_code'),
        amount=F('obligations_incurred_total_by_award_cpe')
    ).values('id', 'type', 'name', 'code', 'amount').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    budget_sub_function_total = budget_sub_function.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in budget_sub_function_total.items():
        budget_sub_function_total = value

    # Unpack federal account object class results
    federal_accounts_results, object_classes_results, recipients_results,\
        program_activity_results, award_category_results, awards_results, \
        awarding_top_tier_agencies_results,\
        awarding_sub_tier_agencies_results = federal_account_budget(queryset, fiscal_year)

    budget_sub_function_results = {
        'total': budget_sub_function_total,
        'end_date': fiscal_year,
        'results': budget_sub_function
    }
    results = [
        budget_sub_function_results,
        federal_accounts_results,
        program_activity_results,
        object_classes_results,
        recipients_results,
        award_category_results,
        awards_results,
        awarding_top_tier_agencies_results,
        awarding_sub_tier_agencies_results
    ]
    return results
