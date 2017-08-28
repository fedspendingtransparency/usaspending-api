from django.db.models import F, Sum


def award_category(queryset, fiscal_year):
    # Award Category Queryset
    award_categories = queryset.annotate(
        recipient_unique_id=F('award__recipient__recipient_unique_id'),
        award_category=F('award__category')
    ).values(
        'recipient_unique_id', 'award_category').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    award_category_total = award_categories.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in award_category_total.items():
        award_category_total = value

    # Unpack awards
    awards_results = award(queryset, fiscal_year)

    award_category_results = {
        'count': award_categories.count(),
        'total': award_category_total,
        'end_date': fiscal_year,
        'award_category': award_categories,
        'awards': awards_results
    }
    results = [
        award_category_results
    ]
    return results


def award(queryset, fiscal_year):
    # Awards Queryset
    awards = queryset.annotate(
        award_category=F('award__category'),
        award_piid=F('award__piid'),
        parent_award=F('award__parent_award'),
        award_fain=F('award__fain'),
        award_uri=F('award__uri')
    ).values(
        'award_category', 'award_piid', 'parent_award', 'award_fain', 'award_uri').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    awards_total = awards.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in awards_total.items():
        awards_total = value

    awards_results = {
        'count': awards.count(),
        'total': awards_total,
        'end_date': fiscal_year,
        'awards': awards,
    }
    return awards_results
