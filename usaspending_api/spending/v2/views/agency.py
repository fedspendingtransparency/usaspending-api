from django.db.models import F, Sum, Value, CharField


def awarding_agency(queryset, end_date):
    # Awarding Agencies Queryset
    awarding_agencies = queryset.annotate(
        id=F('treasury_account__awarding_toptier_agency__toptier_agency_id'),
        type=Value('awarding_agency', output_field=CharField()),
        code=F('treasury_account__awarding_toptier_agency__toptier_agency_id'),
        name=F('treasury_account__awarding_toptier_agency__name'),
        amount=Sum('obligations_incurred_by_program_object_class_cpe')
    ).values('id', 'type', 'code', 'name', 'amount').annotate(
        total=Sum('obligations_incurred_by_program_object_class_cpe')
    ).order_by('-total')

    awarding_agencies_total = awarding_agencies.aggregate(
        Sum('obligations_incurred_by_program_object_class_cpe')
    )
    for key, value in awarding_agencies_total.items():
        awarding_agencies_total = value

    awarding_agencies_results = {
        'total': awarding_agencies_total,
        'end_date': end_date,
        'results': awarding_agencies,
    }
    return awarding_agencies_results


def awarding_top_tier_agency(queryset, end_date):
    # Awarding Top Tier Agencies Queryset
    awarding_top_tier_agencies = queryset.annotate(
        id=F('treasury_account__awarding_toptier_agency__toptier_agency_id'),
        type=Value('top_tier_agency', output_field=CharField()),
        code=F('treasury_account__awarding_toptier_agency__cgac_code'),
        name=F('treasury_account__awarding_toptier_agency__name'),
        amount=Sum('obligations_incurred_by_program_object_class_cpe')
    ).values(
        'id', 'type', 'code', 'name', 'amount'
    ).annotate(
        total=Sum('obligations_incurred_by_program_object_class_cpe')).order_by('-total')

    awarding_top_tier_agencies_total = awarding_top_tier_agencies.aggregate(
        Sum('obligations_incurred_by_program_object_class_cpe')
    )
    for key, value in awarding_top_tier_agencies_total.items():
        awarding_top_tier_agencies_total = value

    awarding_top_tier_agencies_results = {
        'total': awarding_top_tier_agencies_total,
        'end_date': end_date,
        'results': awarding_top_tier_agencies,
    }
    return awarding_top_tier_agencies_results


def awarding_sub_tier_agency(queryset, end_date):
    # Awarding Sub Tier Agencies Queryset
    awarding_sub_tier_agencies = queryset.filter(
        award__period_of_performance_current_end_date=end_date
    ).annotate(
        id=F('treasury_account__awarding_toptier_agency__cgac_code'),
        type=Value('sub_tier_agency', output_field=CharField()),
        code=F('award__awarding_agency__subtier_agency__subtier_code'),
        name=F('award__awarding_agency__subtier_agency__name'),
        amount=Sum('transaction_obligated_amount')
    ).values(
        'id', 'type', 'code', 'name', 'amount'
    ).annotate(
        total=Sum('transaction_obligated_amount')).order_by('-total')

    awarding_sub_tier_agencies_total = awarding_sub_tier_agencies.aggregate(
        Sum('transaction_obligated_amount')
    )
    for key, value in awarding_sub_tier_agencies_total.items():
        awarding_sub_tier_agencies_total = value

    awarding_sub_tier_agencies_results = {
        'total': awarding_sub_tier_agencies_total,
        'end_date': end_date,
        'results': awarding_sub_tier_agencies,
    }
    return awarding_sub_tier_agencies_results
