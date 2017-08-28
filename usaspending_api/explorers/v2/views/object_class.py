from django.db.models import F, Sum
from datetime import datetime

from usaspending_api.explorers.v2.filters.fy_filter import fy_filter
from usaspending_api.explorers.v2.views.recipient import recipient_budget


def object_class_budget(queryset):
    fiscal_year = fy_filter(datetime.now().date())
    # Object Classes Queryset
    object_classes = queryset.annotate(
        main_account_code=F('treasury_account__federal_account__main_account_code'),
        major_object_class_name=F('object_class__major_object_class_name'),
        major_object_class_code=F('object_class__major_object_class')
    ).values(
        'main_account_code', 'major_object_class_name', 'major_object_class_code').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    object_classes_total = object_classes.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in object_classes_total.items():
        object_classes_total = value

    object_classes_results = {
        'count': object_classes.count(),
        'total': object_classes_total,
        'end_date': fiscal_year,
        'object_classes': object_classes,
    }

    # Unpack recipient results
    recipients_results, award_category_results, awards_results = recipient_budget(queryset)

    results = [
        object_classes_results,
        recipients_results,
        award_category_results,
        awards_results
    ]
    return results


def object_class_pa(queryset):
    fiscal_year = fy_filter(datetime.now().date())
    # Object Classes Queryset
    object_classes = queryset.annotate(
        program_activity_code=F('program_activity__program_activity_code'),
        major_object_class_name=F('object_class__major_object_class_name'),
        major_object_class_code=F('object_class__major_object_class')
    ).values(
        'program_activity_code', 'major_object_class_name', 'major_object_class_code').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    object_classes_total = object_classes.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in object_classes_total.items():
        object_classes_total = value

    object_classes_results = {
        'count': object_classes.count(),
        'total': object_classes_total,
        'end_date': fiscal_year,
        'object_classes': object_classes,
    }

    # Unpack recipient results
    recipients_results, award_category_results, awards_results = recipient_budget(queryset)

    results = [
        object_classes_results,
        recipients_results,
        award_category_results,
        awards_results
    ]
    return results
