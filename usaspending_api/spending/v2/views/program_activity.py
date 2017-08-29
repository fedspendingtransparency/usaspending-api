from datetime import datetime

from django.db.models import F, Sum, CharField, Value

from usaspending_api.spending.v2.filters.fy_filter import fy_filter
from usaspending_api.spending.v2.views.object_class import object_class_budget


def program_activity(queryset):
    fiscal_year = fy_filter(datetime.now().date())
    # Program Activity Queryset
    pa = queryset.annotate(
        id=F('financial_accounts_by_awards_id'),
        type=Value('program_activity', output_field=CharField()),
        name=F('program_activity__program_activity_name'),
        code=F('program_activity__program_activity_code'),
        amount=F('obligations_incurred_total_by_award_cpe')
    ).values(
        'id', 'type', 'name', 'code', 'amount').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    program_activity_total = pa.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in program_activity_total.items():
        program_activity_total = value

    # Unpack object class program activity results
    object_classes_results, recipients_results, award_category_results, awards_results, \
        awarding_top_tier_agencies_results, awarding_sub_tier_agencies_results = object_class_budget(queryset)

    program_activity_results = {
        'total': program_activity_total,
        'end_date': fiscal_year,
        'results': pa,
    }
    results = [
        program_activity_results,
        object_classes_results,
        recipients_results,
        award_category_results,
        awards_results,
        awarding_top_tier_agencies_results,
        awarding_sub_tier_agencies_results
    ]
    return results
