from django.db.models import F, Sum

from usaspending_api.spending.v2.views.budget_function_explorer.object_class import object_class_budget_pa


def program_activity_budget(queryset, fiscal_year):
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

    # Unpack object class program activity results
    object_classes_results = object_class_budget_pa(queryset, fiscal_year)

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
