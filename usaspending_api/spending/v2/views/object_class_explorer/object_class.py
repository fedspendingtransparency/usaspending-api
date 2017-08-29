from django.db.models import F, Sum

from usaspending_api.spending.v2.views.object_class_explorer.agency import funding_agency, awarding_agency


def object_class(queryset, fiscal_year):
    # Object Classes Queryset
    object_classes = queryset.annotate(
        major_object_class_name=F('object_class__major_object_class_name'),
        major_object_class_code=F('object_class__major_object_class')
    ).values(
        'major_object_class_name', 'major_object_class_code').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    object_classes_total = object_classes.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in object_classes_total.items():
        object_classes_total = value

    # Unpack funding agency results
    funding_agencies_results = funding_agency(queryset, fiscal_year)

    # Unpack awarding agency results
    awarding_agencies_results = awarding_agency(queryset, fiscal_year)

    object_classes_results = {
        'count': object_classes.count(),
        'total': object_classes_total,
        'end_date': fiscal_year,
        'object_classes': object_classes,
        'awarding_agencies': awarding_agencies_results,
        'funding_agencies': funding_agencies_results
    }
    results = [
        object_classes_results
    ]
    return results
