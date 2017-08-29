from django.db.models import F, Sum, Value, CharField

from usaspending_api.spending.v2.views.budget_subfunction import budget_subfunction


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

    # Unpack budget_subfunction results
    budget_sub_function_results, federal_accounts_results, object_classes_results, \
        recipients_results, program_activity_results, award_category_results, awards_results, \
        awarding_top_tier_agencies_results,\
        awarding_sub_tier_agencies_results = budget_subfunction(queryset, fiscal_year)

    budget_function_results = {
        'total': function_total,
        'end_date': fiscal_year,
        'results': bf
    }
    results = [
        budget_function_results,
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
