from django.db.models import F, Sum, Value, CharField


def agency(queryset, date):
    # Awarding Agencies Queryset
    queryset = queryset.annotate(
        id=F('treasury_account__awarding_toptier_agency__toptier_agency_id'),
        type=Value('agency', output_field=CharField()),
        code=F('treasury_account__awarding_toptier_agency__toptier_agency_id'),
        name=F('treasury_account__awarding_toptier_agency__name')
    ).values('id', 'type', 'code', 'name', 'amount').annotate(
        total=Sum('obligations_incurred_by_program_object_class_cpe')
    ).order_by('-total')

    total = queryset.aggregate(
        Sum('obligations_incurred_by_program_object_class_cpe')
    )
    for key, value in total.items():
        total = value

    return total, date, queryset


def awarding_top_tier_agency(queryset, date):
    # Awarding Top Tier Agencies Queryset
    queryset = queryset.annotate(
        id=F('treasury_account__awarding_toptier_agency__cgac_code'),
        type=Value('top_tier_agency', output_field=CharField()),
        code=F('treasury_account__awarding_toptier_agency__cgac_code'),
        name=F('treasury_account__awarding_toptier_agency__name')
    ).values(
        'id', 'type', 'code', 'name', 'amount'
    ).annotate(
        total=Sum('obligations_incurred_by_program_object_class_cpe')).order_by('-total')

    total = queryset.aggregate(
        Sum('obligations_incurred_by_program_object_class_cpe')
    )
    for key, value in total.items():
        total = value

    return total, date, queryset


def awarding_sub_tier_agency(alt_set, date):
    # Awarding Sub Tier Agencies Queryset
    alt_set = alt_set.annotate(
        id=F('award__awarding_agency__subtier_agency__subtier_code'),
        type=Value('sub_tier_agency', output_field=CharField()),
        code=F('award__awarding_agency__subtier_agency__subtier_code'),
        name=F('award__awarding_agency__subtier_agency__name')
    ).values(
        'id', 'type', 'code', 'name', 'amount'
    ).annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    total = alt_set.aggregate(
        Sum('obligations_incurred_total_by_award_cpe')
    )
    for key, value in total.items():
        total = value

    return total, date, alt_set
