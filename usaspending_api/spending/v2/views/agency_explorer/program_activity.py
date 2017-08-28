from django.db.models import F, Sum

from usaspending_api.spending.v2.views.agency_explorer.object_class import object_class_awarding, object_class_funding


def program_activity_awarding(queryset, fiscal_year):
    # Program Activity Queryset
    pa = queryset.annotate(
        awarding_agency_id=F('award__awarding_agency__id'),
        main_account_code=F('treasury_account__federal_account__main_account_code'),
        program_activity_name=F('program_activity__program_activity_name'),
        program_activity_code=F('program_activity__program_activity_code')
    ).values(
        'awarding_agency_id', 'main_account_code', 'program_activity_name', 'program_activity_code').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    program_activity_total = pa.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in program_activity_total.items():
        program_activity_total = value

    # Unpack object class program activity results
    object_classes_results = object_class_awarding(queryset, fiscal_year)

    program_activity_results = {
        'count': pa.count(),
        'total': program_activity_total,
        'end_date': fiscal_year,
        'program_activity': pa,
        'object_class': object_classes_results
    }
    results = [
        program_activity_results
    ]
    return results


def program_activity_funding(queryset, fiscal_year):
    # Program Activity Queryset
    pa = queryset.annotate(
        funding_agency_id=F('award__funding_agency__id'),
        main_account_code=F('treasury_account__federal_account__main_account_code'),
        program_activity_name=F('program_activity__program_activity_name'),
        program_activity_code=F('program_activity__program_activity_code')
    ).values(
        'funding_agency_id', 'main_account_code', 'program_activity_name', 'program_activity_code').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    program_activity_total = pa.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in program_activity_total.items():
        program_activity_total = value

    # Unpack object class program activity results
    object_classes_results = object_class_funding(queryset, fiscal_year)

    program_activity_results = {
        'count': pa.count(),
        'total': program_activity_total,
        'end_date': fiscal_year,
        'program_activity': pa,
        'object_class': object_classes_results
    }
    results = [
        program_activity_results
    ]
    return results
