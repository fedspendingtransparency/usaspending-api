from django.db.models import F, Sum

from usaspending_api.spending.v2.views.object_class_explorer.federal_account import federal_account_funding, \
    federal_account_awarding


def awarding_agency(queryset, fiscal_year):
    # Awarding Agencies Queryset
    awarding_agencies = queryset.annotate(
        major_object_class_code=F('object_class__major_object_class'),
        awarding_agency_id=F('award__awarding_agency__id')
    ).values(
        'major_object_class_code', 'awarding_agency_id'
    ).annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    awarding_agencies_total = awarding_agencies.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in awarding_agencies_total.items():
        awarding_agencies_total = value

    # Unpack awarding top and sub tier results
    awarding_top_tier_agencies_results = awarding_top_tier_agency(queryset, fiscal_year)

    awarding_agencies_results = {
        'count': awarding_agencies.count(),
        'total': awarding_agencies_total,
        'end_date': fiscal_year,
        'awarding_agencies': awarding_agencies,
        'awarding_top_tier_agencies': awarding_top_tier_agencies_results
    }
    results = [
        awarding_agencies_results
    ]
    return results


def awarding_top_tier_agency(queryset, fiscal_year):
    # Awarding Top Tier Agencies Queryset
    awarding_top_tier_agencies = queryset.annotate(
        major_object_class_code=F('object_class__major_object_class'),
        awarding_agency_id=F('award__awarding_agency__id'),
        awarding_toptier_agency_id=F('award__awarding_agency__toptier_agency__toptier_agency_id'),
        awarding_toptier_agency_name=F('award__awarding_agency__toptier_agency__name')
    ).values(
        'major_object_class_code', 'awarding_agency_id', 'awarding_toptier_agency_id',
        'awarding_toptier_agency_name'
    ).annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    awarding_top_tier_agencies_total = awarding_top_tier_agencies.aggregate(
        Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in awarding_top_tier_agencies_total.items():
        awarding_top_tier_agencies_total = value

    # Unpack awarding sub tier results
    awarding_sub_tier_agencies_results = awarding_sub_tier_agency(queryset, fiscal_year)

    awarding_top_tier_agencies_results = {
        'count': awarding_top_tier_agencies.count(),
        'total': awarding_top_tier_agencies_total,
        'end_date': fiscal_year,
        'awarding_top_tier_agencies': awarding_top_tier_agencies,
        'awarding_sub_tier_agencies': awarding_sub_tier_agencies_results
    }
    results = [
        awarding_top_tier_agencies_results
    ]
    return results


def awarding_sub_tier_agency(queryset, fiscal_year):
    # Awarding Sub Tier Agencies Queryset
    awarding_sub_tier_agencies = queryset.annotate(
        major_object_class_code=F('object_class__major_object_class'),
        awarding_agency_id=F('award__awarding_agency__id'),
        awarding_toptier_agency_id=F('award__awarding_agency__toptier_agency__toptier_agency_id'),
        awarding_toptier_agency_name=F('award__awarding_agency__toptier_agency__name'),
        awarding_subtier_agency_id=F('award__awarding_agency__subtier_agency__subtier_code'),
        awarding_subtier_agency_name=F('award__awarding_agency__subtier_agency__name'),
    ).values(
        'major_object_class_code', 'awarding_agency_id', 'awarding_toptier_agency_id',
        'awarding_toptier_agency_name', 'awarding_subtier_agency_id', 'awarding_subtier_agency_name'
    ).annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    awarding_sub_tier_agencies_total = awarding_sub_tier_agencies.aggregate(
        Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in awarding_sub_tier_agencies_total.items():
        awarding_sub_tier_agencies_total = value

    # Unpack federal account by agency
    federal_accounts_results = federal_account_awarding(queryset, fiscal_year)

    awarding_sub_tier_agencies_results = {
        'count': awarding_sub_tier_agencies.count(),
        'total': awarding_sub_tier_agencies_total,
        'end_date': fiscal_year,
        'awarding_sub_tier_agencies': awarding_sub_tier_agencies,
        'federal_account': federal_accounts_results
    }
    results = [
        awarding_sub_tier_agencies_results
    ]
    return results


def funding_agency(queryset, fiscal_year):
    # Funding Agencies Queryset
    funding_agencies = queryset.annotate(
        major_object_class_code=F('object_class__major_object_class'),
        funding_agency_id=F('award__funding_agency__id'),
    ).values(
        'major_object_class_code', 'funding_agency_id'
    ).annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    funding_agencies_total = funding_agencies.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in funding_agencies_total.items():
        funding_agencies_total = value

    # Unpack funding top and sub tier results
    funding_top_tier_agencies_results = funding_top_tier_agency(queryset, fiscal_year)

    funding_agencies_results = {
        'count': funding_agencies.count(),
        'total': funding_agencies_total,
        'end_date': fiscal_year,
        'funding_agencies': funding_agencies,
        'funding_top_tier_agencies': funding_top_tier_agencies_results
    }
    results = [
        funding_agencies_results
    ]
    return results


def funding_top_tier_agency(queryset, fiscal_year):
    # Funding Top Tier Agencies Queryset
    funding_top_tier_agencies = queryset.annotate(
        major_object_class_code=F('object_class__major_object_class'),
        funding_agency_id=F('award__funding_agency__id'),
        funding_toptier_agency_id=F('award__funding_agency__toptier_agency__toptier_agency_id'),
        funding_toptier_agency_name=F('award__funding_agency__toptier_agency__name')
    ).values(
        'major_object_class_code', 'funding_agency_id',
        'funding_toptier_agency_id', 'funding_toptier_agency_name'
    ).annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    funding_top_tier_agencies_total = funding_top_tier_agencies.aggregate(
        Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in funding_top_tier_agencies_total.items():
        funding_top_tier_agencies_total = value

    # Unpack funding sub tier results
    funding_sub_tier_agencies_results = funding_sub_tier_agency(queryset, fiscal_year)

    funding_top_tier_agencies_results = {
        'count': funding_top_tier_agencies.count(),
        'total': funding_top_tier_agencies_total,
        'end_date': fiscal_year,
        'funding_top_tier_agencies': funding_top_tier_agencies,
        'funding_sub_tier_agencies': funding_sub_tier_agencies_results
    }
    results = [
        funding_top_tier_agencies_results
    ]
    return results


def funding_sub_tier_agency(queryset, fiscal_year):
    # Funding Sub Tier Agencies Queryset
    funding_sub_tier_agencies = queryset.annotate(
        major_object_class_code=F('object_class__major_object_class'),
        funding_agency_id=F('award__funding_agency__id'),
        funding_toptier_agency_id=F('award__funding_agency__toptier_agency__toptier_agency_id'),
        funding_toptier_agency_name=F('award__funding_agency__toptier_agency__name'),
        funding_subtier_agency_id=F('award__funding_agency__subtier_agency__subtier_code'),
        funding_subtier_agency_name=F('award__funding_agency__subtier_agency__name')
    ).values(
        'major_object_class_code', 'funding_agency_id', 'funding_toptier_agency_id',
        'funding_toptier_agency_name', 'funding_subtier_agency_id', 'funding_subtier_agency_name'
    ).annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    funding_sub_tier_agencies_total = funding_sub_tier_agencies.aggregate(
        Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in funding_sub_tier_agencies_total.items():
        funding_sub_tier_agencies_total = value

    # Unpack federal account by agency
    federal_accounts_results = federal_account_funding(queryset, fiscal_year)

    funding_sub_tier_agencies_results = {
        'count': funding_sub_tier_agencies.count(),
        'total': funding_sub_tier_agencies_total,
        'end_date': fiscal_year,
        'funding_sub_tier_agencies': funding_sub_tier_agencies,
        'federal_account': federal_accounts_results
    }
    results = [
        funding_sub_tier_agencies_results
    ]
    return results
