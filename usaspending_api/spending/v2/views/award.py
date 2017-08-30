from django.db.models import F, Sum, Value, CharField


def award_category(queryset, fiscal_year):
    # Award Category Queryset
    award_categories = queryset.annotate(
        id=F('award__recipient__recipient_unique_id'),
        type=Value('award_category', output_field=CharField()),
        code=F('award'),
        name=F('award__category'),
        amount=F('obligations_incurred_total_by_award_cpe')
    ).values(
        'id', 'type', 'code', 'name', 'amount').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    award_category_total = award_categories.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in award_category_total.items():
        award_category_total = value

    award_category_results = {
        'total': award_category_total,
        'end_date': fiscal_year,
        'results': award_categories,
    }
    return award_category_results


def award(queryset, fiscal_year):
    # Awards Queryset
    awards = queryset.annotate(
        id=F('award'),
        type=Value('award', output_field=CharField()),
        award_piid=F('award__piid'),
        award_fain=F('award__fain'),
        award_uri=F('award__uri'),
        name=F('award__category'),
        amount=F('obligations_incurred_total_by_award_cpe')
    ).values(
        'id', 'type', 'award_piid', 'award_fain', 'award_uri', 'name', 'amount').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    awards_total = awards.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in awards_total.items():
        awards_total = value

    awards_results = {
        'total': awards_total,
        'end_date': fiscal_year,
        'results': awards,
    }
    return awards_results
