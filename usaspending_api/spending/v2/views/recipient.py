from django.db.models import F, Sum, Value, CharField
from datetime import datetime

from usaspending_api.spending.v2.filters.fy_filter import fy_filter
from usaspending_api.spending.v2.views.award import award_category


def recipient_budget(queryset):
    fiscal_year = fy_filter(datetime.now().date())
    # Recipients Queryset
    recipients = queryset.annotate(
        id=F('financial_accounts_by_awards_id'),
        type=Value('recipient', output_field=CharField()),
        name=F('award__recipient__recipient_name'),
        code=F('award__recipient__recipient_unique_id'),
        amount=F('obligations_incurred_total_by_award_cpe')
    ).values(
        'id', 'type', 'name', 'code', 'amount').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    recipients_total = recipients.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in recipients_total.items():
        recipients_total = value

    # Unpack award results
    award_category_results, awards_results, awarding_top_tier_agencies_results,\
        awarding_sub_tier_agencies_results = award_category(queryset)

    recipients_results = {
        'total': recipients_total,
        'end_date': fiscal_year,
        'results': recipients
    }
    results = [
        recipients_results,
        award_category_results,
        awards_results,
        awarding_top_tier_agencies_results,
        awarding_sub_tier_agencies_results
    ]
    return results
