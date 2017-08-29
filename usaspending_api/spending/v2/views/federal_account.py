from datetime import datetime

from django.db.models import F, Sum, CharField, Value

from usaspending_api.spending.v2.filters.fy_filter import fy_filter
from usaspending_api.spending.v2.views.program_activity import program_activity


def federal_account_budget(queryset):
    fiscal_year = fy_filter(datetime.now().date())
    # Federal Account Queryset
    federal_accounts = queryset.annotate(
        id=F('financial_accounts_by_awards_id'),
        type=Value('federal_account', output_field=CharField()),
        name=F('treasury_account__federal_account__account_title'),
        code=F('treasury_account__federal_account__main_account_code'),
        amount=F('obligations_incurred_total_by_award_cpe')
    ).values(
        'id', 'type', 'name', 'code', 'amount').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    federal_accounts_total = federal_accounts.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in federal_accounts_total.items():
        federal_accounts_total = value

    # Unpack program activity results
    program_activity_results, object_classes_results, recipients_results,\
        award_category_results, awards_results, awarding_top_tier_agencies_results,\
        awarding_sub_tier_agencies_results = program_activity(queryset)

    federal_accounts_results = {
        'total': federal_accounts_total,
        'end_date': fiscal_year,
        'results': federal_accounts
    }
    results = [
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
