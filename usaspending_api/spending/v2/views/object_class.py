from django.db.models import F, Sum, CharField, Value


def object_class_budget(queryset, fiscal_year):
    # Object Classes Queryset
    object_classes = queryset.annotate(
        id=F('program_activity__program_activity_code'),
        type=Value('object_class', output_field=CharField()),
        name=F('object_class__major_object_class_name'),
        code=F('object_class__major_object_class'),
        amount=Sum('obligations_incurred_by_program_object_class_cpe')
    ).values(
        'id', 'type', 'name', 'code', 'amount').annotate(
        total=Sum('obligations_incurred_by_program_object_class_cpe')).order_by('-total')

    object_classes_total = object_classes.aggregate(Sum('obligations_incurred_by_program_object_class_cpe'))
    for key, value in object_classes_total.items():
        object_classes_total = value

    object_classes_results = {
        'total': object_classes_total,
        'end_date': fiscal_year,
        'results': object_classes
    }
    return object_classes_results
