from django.db.models import F, Sum, Value, CharField


def recipient(alt_set, fiscal_date):
    # Recipients Queryset
    alt_set = alt_set.annotate(
        id=F('award__recipient__recipient_unique_id'),
        type=Value('recipient', output_field=CharField()),
        name=F('award__recipient__recipient_name'),
        code=F('award__recipient__recipient_unique_id')
    ).values(
        'id', 'type', 'name', 'code', 'amount').annotate(
        total=Sum('transaction_obligated_amount')).order_by('-total')

    total = alt_set.aggregate(Sum('transaction_obligated_amount'))
    for key, value in total.items():
        total = value

    return total, fiscal_date, alt_set
