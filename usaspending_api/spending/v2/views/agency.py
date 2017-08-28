from django.db.models import F, Sum
from datetime import datetime

from usaspending_api.spending.v2.filters.fy_filter import fy_filter
from usaspending_api.spending.v2.views.federal_account import federal_account_agency


def funding_agency(queryset):
    fiscal_year = fy_filter(datetime.now().date())
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

    funding_agencies_results = {
        'count': funding_agencies.count(),
        'total': funding_agencies_total,
        'end_date': fiscal_year,
        'funding_agencies': funding_agencies,
    }
    # Unpack funding top and sub tier results
    funding_top_tier_agencies_results,\
        funding_sub_tier_agency_results = funding_top_tier_agency(queryset)

    results = [
        funding_agencies_results,
        funding_top_tier_agencies_results,
        funding_sub_tier_agency_results
    ]
    return results


def funding_top_tier_agency(queryset):
    fiscal_year = fy_filter(datetime.now().date())
    # Funding Top Tier Agencies Queryset
    funding_top_tier_agencies = queryset.annotate(
        major_object_class_code=F('object_class__major_object_class'),
        funding_agency_id=F('award__funding_agency__id'),
        funding_toptier_agency_id=F('award__funding_agency__toptier_agency__toptier_agency_id'),
        funding_toptier_agency_name=F('award__funding_agency__toptier_agency__name')
    ).values(
        'major_object_class_code', 'funding_agency_id', 'funding_toptier_agency_id',
        'funding_toptier_agency_name'
    ).annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    funding_top_tier_agencies_total = funding_top_tier_agencies.aggregate(
        Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in funding_top_tier_agencies_total.items():
        funding_top_tier_agencies_total = value

    funding_top_tier_agencies_results = {
        'count': funding_top_tier_agencies.count(),
        'total': funding_top_tier_agencies_total,
        'end_date': fiscal_year,
        'funding_top_tier_agencies': funding_top_tier_agencies,
    }
    # Unpack funding sub tier results
    funding_sub_tier_agencies_results, federal_accounts_results, program_activity_results,\
        recipients_results, award_category_results, awards_results = funding_sub_tier_agency(queryset)

    results = [
        funding_top_tier_agencies_results,
        funding_sub_tier_agencies_results,
        federal_accounts_results,
        program_activity_results,
        recipients_results,
        award_category_results,
        awards_results
    ]
    return results


def funding_sub_tier_agency(queryset):
    fiscal_year = fy_filter(datetime.now().date())
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

    funding_sub_tier_agencies_results = {
        'count': funding_sub_tier_agencies.count(),
        'total': funding_sub_tier_agencies_total,
        'end_date': fiscal_year,
        'funding_sub_tier_agencies': funding_sub_tier_agencies,
    }
    # Unpack federal account by agency
    federal_accounts_results, program_activity_results, recipients_results, award_category_results, \
        awards_results = federal_account_agency(queryset)

    results = [
        funding_sub_tier_agencies_results,
        federal_accounts_results,
        program_activity_results,
        recipients_results,
        award_category_results,
        awards_results
    ]
    return results


def awarding_agency(queryset):
    fiscal_year = fy_filter(datetime.now().date())
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

    awarding_agencies_results = {
        'count': awarding_agencies.count(),
        'total': awarding_agencies_total,
        'end_date': fiscal_year,
        'awarding_agencies': awarding_agencies,
    }
    # Unpack awarding top and sub tier results
    awarding_top_tier_agencies_results,\
        awarding_sub_tier_agency_results = awarding_top_tier_agency(queryset)

    results = [
        awarding_agencies_results,
        awarding_top_tier_agencies_results,
        awarding_sub_tier_agency_results
    ]
    return results


def awarding_top_tier_agency(queryset):
    fiscal_year = fy_filter(datetime.now().date())
    # Awarding Top Tier Agencies Queryset
    awarding_top_tier_agencies = queryset.annotate(
        major_object_class_code=F('object_class__major_object_class'),
        awarding_agency_id=F('award__awarding_agency__id'),
        awarding_toptier_agency_id=F('award__awarding_agency__toptier_agency__toptier_agency_id'),
        awarding_toptier_agency_name=F('award__awarding_agency__toptier_agency__name')
    ).values(
        'major_object_class_code', 'awarding_agency_id',
        'awarding_toptier_agency_id', 'awarding_toptier_agency_name'
    ).annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    awarding_top_tier_agencies_total = awarding_top_tier_agencies.aggregate(
        Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in awarding_top_tier_agencies_total.items():
        awarding_top_tier_agencies_total = value

    awarding_top_tier_agencies_results = {
        'count': awarding_top_tier_agencies.count(),
        'total': awarding_top_tier_agencies_total,
        'end_date': fiscal_year,
        'awarding_top_tier_agencies': awarding_top_tier_agencies,
    }
    # Unpack awarding sub tier results
    awarding_sub_tier_agencies_results, federal_accounts_results, program_activity_results,\
        recipients_results, award_category_results, awards_results = awarding_sub_tier_agency(queryset)

    results = [
        awarding_top_tier_agencies_results,
        awarding_sub_tier_agencies_results,
        federal_accounts_results,
        program_activity_results,
        recipients_results,
        award_category_results,
        awards_results
    ]
    return results


def awarding_sub_tier_agency(queryset):
    fiscal_year = fy_filter(datetime.now().date())
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

    awarding_sub_tier_agencies_results = {
        'count': awarding_sub_tier_agencies.count(),
        'total': awarding_sub_tier_agencies_total,
        'end_date': fiscal_year,
        'awarding_sub_tier_agencies': awarding_sub_tier_agencies,
    }
    # Unpack federal account by agency
    federal_accounts_results, program_activity_results, recipients_results, award_category_results,\
        awards_results = federal_account_agency(queryset)

    results = [
        awarding_sub_tier_agencies_results,
        federal_accounts_results,
        program_activity_results,
        recipients_results,
        award_category_results,
        awards_results
    ]
    return results
