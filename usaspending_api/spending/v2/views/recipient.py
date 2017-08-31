from django.db.models import F, Sum, Value, CharField


def recipient_budget(queryset, fiscal_year):
    # Recipients Queryset
    recipients = queryset.filter(
        award__period_of_performance_current_end_date=fiscal_year
    ).annotate(
        id=F('object_class__major_object_class'),
        type=Value('recipient', output_field=CharField()),
        name=F('award__recipient__recipient_name'),
        code=F('award__recipient__recipient_unique_id'),
        amount=Sum('transaction_obligated_amount')
    ).values(
        'id', 'type', 'name', 'code', 'amount').annotate(
        total=Sum('transaction_obligated_amount')).order_by('-total')

    recipients_total = recipients.aggregate(Sum('transaction_obligated_amount'))
    for key, value in recipients_total.items():
        recipients_total = value

    recipients_results = {
        'total': recipients_total,
        'end_date': fiscal_year,
        'results': recipients
    }
    return recipients_results
