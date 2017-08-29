from django.db.models import F, Sum

from usaspending_api.spending.v2.views.object_class_explorer.program_activity import program_activity_awarding, \
    program_activity_funding


def federal_account_awarding(queryset, fiscal_year):
    # Federal Account Queryset
    federal_accounts = queryset.annotate(
        major_object_class_code=F('object_class__major_object_class'),
        awarding_agency_id=F('award__awarding_agency__id'),
        federal_account_name=F('treasury_account__federal_account__account_title'),
        federal_account_agency_id=F('treasury_account__federal_account__agency_identifier'),
        main_account_code=F('treasury_account__federal_account__main_account_code')
    ).values(
        'major_object_class_code', 'awarding_agency_id', 'federal_account_name',
        'federal_account_agency_id', 'main_account_code').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    federal_accounts_total = federal_accounts.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in federal_accounts_total.items():
        federal_accounts_total = value

    # Unpack object class federal account results
    program_activity_results = program_activity_awarding(queryset, fiscal_year)

    federal_accounts_results = {
        'count': federal_accounts.count(),
        'total': federal_accounts_total,
        'end_date': fiscal_year,
        'federal_account': federal_accounts,
        'program_activity': program_activity_results
    }
    results = [
        federal_accounts_results
    ]
    return results


def federal_account_funding(queryset, fiscal_year):
    # Federal Account Queryset
    federal_accounts = queryset.annotate(
        major_object_class_code=F('object_class__major_object_class'),
        funding_agency_id=F('award__funding_agency__id'),
        federal_account_name=F('treasury_account__federal_account__account_title'),
        federal_account_agency_id=F('treasury_account__federal_account__agency_identifier'),
        main_account_code=F('treasury_account__federal_account__main_account_code')
    ).values(
        'major_object_class_code', 'funding_agency_id', 'federal_account_name',
        'federal_account_agency_id', 'main_account_code').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    federal_accounts_total = federal_accounts.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in federal_accounts_total.items():
        federal_accounts_total = value

    # Unpack object class federal account results
    program_activity_results = program_activity_funding(queryset, fiscal_year)

    federal_accounts_results = {
        'count': federal_accounts.count(),
        'total': federal_accounts_total,
        'end_date': fiscal_year,
        'federal_account': federal_accounts,
        'program_activity': program_activity_results
    }
    results = [
        federal_accounts_results
    ]
    return results
