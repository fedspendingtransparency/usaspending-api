from django.db.models import F, Sum, Value, CharField


def award_category(queryset, end_date):
    # Award Category Queryset
    award_categories = queryset.filter(
        award__period_of_performance_current_end_date=end_date
    ).annotate(
        id=F('award__recipient__recipient_unique_id'),
        type=Value('award_category', output_field=CharField()),
        code=F('treasury_account__awarding_toptier_agency__toptier_agency_id'),
        name=F('award__category'),
        amount=Sum('transaction_obligated_amount')
    ).values(
        'id', 'type', 'code', 'name', 'amount').annotate(
        total=Sum('transaction_obligated_amount')).order_by('-total')

    award_category_total = award_categories.aggregate(Sum('transaction_obligated_amount'))
    for key, value in award_category_total.items():
        award_category_total = value

    award_category_results = {
        'total': award_category_total,
        'end_date': end_date,
        'results': award_categories,
    }
    return award_category_results


def award(queryset, end_date):
    # Awards Queryset
    awards = queryset.filter(
        award__period_of_performance_current_end_date=end_date
    ).annotate(
        id=F('award__recipient__recipient_unique_id'),
        type=Value('award', output_field=CharField()),
        code=F('treasury_account__awarding_toptier_agency__toptier_agency_id'),
        name=F('award__description'),
        amount=Sum('transaction_obligated_amount')
    ).values(
        'id', 'type', 'code', 'name', 'amount').annotate(
        total=Sum('transaction_obligated_amount')).order_by('-total')

    awards_total = awards.aggregate(Sum('transaction_obligated_amount'))
    for key, value in awards_total.items():
        awards_total = value

    awards_results = {
        'total': awards_total,
        'end_date': end_date,
        'results': awards,
    }
    return awards_results
