from django.db.models import F, Sum, Value, CharField


def awarding_agency(queryset, fiscal_year):
    # Awarding Agencies Queryset
    awarding_agencies = queryset.annotate(
        id=F('award__recipient__recipient_unique_id'),
        type=Value('awarding_agency', output_field=CharField()),
        code=F('award__awarding_agency__id'),
        name=F('award__awarding_agency__toptier_agency__name'),
        amount=F('obligations_incurred_total_by_award_cpe')
    ).values(
        'id', 'type', 'code', 'name', 'amount'
    ).annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    awarding_agencies_total = awarding_agencies.aggregate(
        Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in awarding_agencies_total.items():
        awarding_agencies_total = value

    awarding_agencies_results = {
        'total': awarding_agencies_total,
        'end_date': fiscal_year,
        'results': awarding_agencies,
    }
    return awarding_agencies_results


def awarding_top_tier_agency(queryset, fiscal_year):
    # Awarding Top Tier Agencies Queryset
    awarding_top_tier_agencies = queryset.annotate(
        id=F('award__awarding_agency__id'),
        type=Value('top_tier_agency', output_field=CharField()),
        code=F('award__awarding_agency__toptier_agency__cgac_code'),
        name=F('award__awarding_agency__toptier_agency__name'),
        amount=F('obligations_incurred_total_by_award_cpe')
    ).values(
        'id', 'type', 'code', 'name', 'amount'
    ).annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    awarding_top_tier_agencies_total = awarding_top_tier_agencies.aggregate(
        Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in awarding_top_tier_agencies_total.items():
        awarding_top_tier_agencies_total = value

    awarding_top_tier_agencies_results = {
        'total': awarding_top_tier_agencies_total,
        'end_date': fiscal_year,
        'results': awarding_top_tier_agencies,
    }
    return awarding_top_tier_agencies_results


def awarding_sub_tier_agency(queryset, fiscal_year):
    # Awarding Sub Tier Agencies Queryset
    awarding_sub_tier_agencies = queryset.annotate(
        id=F('award__awarding_agency__toptier_agency__cgac_code'),
        type=Value('sub_tier_agency', output_field=CharField()),
        code=F('award__awarding_agency__subtier_agency__subtier_code'),
        name=F('award__awarding_agency__subtier_agency__name'),
        amount=F('obligations_incurred_total_by_award_cpe')
    ).values(
        'id', 'type', 'code', 'name', 'amount'
    ).annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    awarding_sub_tier_agencies_total = awarding_sub_tier_agencies.aggregate(
        Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in awarding_sub_tier_agencies_total.items():
        awarding_sub_tier_agencies_total = value

    awarding_sub_tier_agencies_results = {
        'total': awarding_sub_tier_agencies_total,
        'end_date': fiscal_year,
        'results': awarding_sub_tier_agencies,
    }
    return awarding_sub_tier_agencies_results
