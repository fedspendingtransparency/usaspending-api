from django.db.models import F, Sum
from datetime import datetime

from usaspending_api.spending.v2.filters.fy_filter import fy_filter
from usaspending_api.spending.v2.views.object_class import object_class_pa
from usaspending_api.spending.v2.views.recipient import recipient


def program_activity(queryset):
    fiscal_year = fy_filter(datetime.now().date())
    # Program Activity Queryset
    pa = queryset.annotate(
        program_activity_name=F('program_activity__program_activity_name'),
        program_activity_code=F('program_activity__program_activity_code')
    ).values(
        'program_activity_name', 'program_activity_code').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    program_activity_total = pa.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in program_activity_total.items():
        program_activity_total = value

    program_activity_results = {
        'count': pa.count(),
        'total': program_activity_total,
        'end_date': fiscal_year,
        'program_activity': pa,
    }

    # Unpack object class program activity results
    object_classes_results, recipients_results, award_category_results,\
        awards_results = object_class_pa(queryset)

    results = [
        program_activity_results,
        object_classes_results,
        recipients_results,
        award_category_results,
        awards_results
    ]
    return results


def program_activity_fa(queryset):
    fiscal_year = fy_filter(datetime.now().date())
    # Program Activity Queryset
    pa = queryset.annotate(
        main_account_code=F('treasury_account__federal_account__main_account_code'),
        program_activity_name=F('program_activity__program_activity_name'),
        program_activity_code=F('program_activity__program_activity_code')
    ).values(
        'main_account_code', 'program_activity_name', 'program_activity_code').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    program_activity_total = pa.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in program_activity_total.items():
        program_activity_total = value

    program_activity_results = {
        'count': pa.count(),
        'total': program_activity_total,
        'end_date': fiscal_year,
        'program_activity': pa,
    }

    # Unpack object class program activity results
    object_classes_results, recipients_results, award_category_results,\
        awards_results = object_class_pa(queryset)

    results = [
        program_activity_results,
        object_classes_results,
        recipients_results,
        award_category_results,
        awards_results
    ]
    return results


def program_activity_oc(queryset):
    fiscal_year = fy_filter(datetime.now().date())
    # Program Activity Queryset
    pa = queryset.annotate(
        main_account_code=F('treasury_account__federal_account__main_account_code'),
        program_activity_name=F('program_activity__program_activity_name'),
        program_activity_code=F('program_activity__program_activity_code')
    ).values(
        'main_account_code', 'program_activity_name', 'program_activity_code').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    program_activity_total = pa.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in program_activity_total.items():
        program_activity_total = value

    program_activity_results = {
        'count': pa.count(),
        'total': program_activity_total,
        'end_date': fiscal_year,
        'program_activity': pa,
    }

    # Unpack object class program activity results
    recipients_results, award_category_results,\
        awards_results = recipient(queryset)

    results = [
        program_activity_results,
        recipients_results,
        award_category_results,
        awards_results
    ]
    return results
