from django.db.models import F, Sum

from usaspending_api.spending.v2.views.budget_function_explorer.recipient import recipient_budget


def object_class_budget(queryset, fiscal_year):
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

    # Unpack recipient results
    recipients_results = recipient_budget(queryset, fiscal_year)

    object_classes_results = {
        'count': object_classes.count(),
        'total': object_classes_total,
        'end_date': fiscal_year,
        'object_classes': object_classes,
        'recipients': recipients_results
    }
    results = [
        object_classes_results,
    ]
    return results


def object_class_budget_pa(queryset, fiscal_year):
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

    # Unpack recipient results
    recipients_results = recipient_budget(queryset, fiscal_year)

    object_classes_results = {
        'count': object_classes.count(),
        'total': object_classes_total,
        'end_date': fiscal_year,
        'object_classes': object_classes,
        'recipients': recipients_results
    }
    results = [
        object_classes_results,
    ]
    return results
