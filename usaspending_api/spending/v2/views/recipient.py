from django.db.models import F, Sum
from datetime import datetime

from usaspending_api.spending.v2.filters.fy_filter import fy_filter
from usaspending_api.spending.v2.views.award import award_category


def recipient(queryset):
    fiscal_year = fy_filter(datetime.now().date())
    # Recipients Queryset
    recipients = queryset.annotate(
        recipient_name=F('award__recipient__recipient_name'),
        recipient_unique_id=F('award__recipient__recipient_unique_id')
    ).values(
        'recipient_name', 'recipient_unique_id').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    recipients_total = recipients.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in recipients_total.items():
        recipients_total = value

    recipients_results = {
        'count': recipients.count(),
        'total': recipients_total,
        'end_date': fiscal_year,
        'recipients': recipients,
    }

    # Unpack award results
    award_category_results, awards_results = award_category(queryset)

    results = [
        recipients_results,
        award_category_results,
        awards_results
    ]
    return results


def recipient_budget(queryset):
    fiscal_year = fy_filter(datetime.now().date())
    # Recipients Queryset
    recipients = queryset.annotate(
        major_object_class_code=F('object_class__major_object_class'),
        recipient_name=F('award__recipient__recipient_name'),
        recipient_unique_id=F('award__recipient__recipient_unique_id')
    ).values(
        'major_object_class_code', 'recipient_name', 'recipient_unique_id').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    recipients_total = recipients.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in recipients_total.items():
        recipients_total = value

    recipients_results = {
        'count': recipients.count(),
        'total': recipients_total,
        'end_date': fiscal_year,
        'recipients': recipients,
    }

    # Unpack award results
    award_category_results, awards_results = award_category(queryset)

    results = [
        recipients_results,
        award_category_results,
        awards_results
    ]
    return results
