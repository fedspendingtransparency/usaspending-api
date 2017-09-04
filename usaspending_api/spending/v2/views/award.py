from django.db.models import F, Sum, Value, CharField


def award_category(alt_set, fiscal_date):
    # Award Category Queryset
    alt_set = alt_set.annotate(
        id=F('award__recipient__recipient_unique_id'),
        type=Value('award_category', output_field=CharField()),
        code=F('treasury_account__awarding_toptier_agency__toptier_agency_id'),
        name=F('award__category')
    ).values(
        'id', 'type', 'code', 'name', 'amount').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    total = alt_set.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in total.items():
        total = value

    return total, fiscal_date, alt_set


def award(alt_set, fiscal_date):
    # Awards Queryset
    alt_set = alt_set.annotate(
        id=F('award__recipient__recipient_unique_id'),
        type=Value('award', output_field=CharField()),
        code=F('treasury_account__awarding_toptier_agency__toptier_agency_id'),
        name=F('award__description')
    ).values(
        'id', 'type', 'code', 'name', 'amount').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    total = alt_set.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in total.items():
        total = value

    return total, fiscal_date, alt_set
