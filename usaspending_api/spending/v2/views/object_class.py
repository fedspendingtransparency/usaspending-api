from django.db.models import F, Sum, CharField, Value


def object_class(queryset, fiscal_date):
    # Object Classes Queryset
    queryset = queryset.annotate(
        id=F('object_class__major_object_class'),
        type=Value('object_class', output_field=CharField()),
        name=F('object_class__major_object_class_name'),
        code=F('object_class__major_object_class'),
        amount=Sum('obligations_incurred_by_program_object_class_cpe')
    ).values(
        'id', 'type', 'name', 'code', 'amount').annotate(
        total=Sum('obligations_incurred_by_program_object_class_cpe')).order_by('-total')

    total = queryset.aggregate(Sum('obligations_incurred_by_program_object_class_cpe'))
    for key, value in total.items():
        total = value

    object_classes_results = {
        'total': total,
        'end_date': fiscal_date,
        'results': queryset
    }
    return total, fiscal_date, queryset
