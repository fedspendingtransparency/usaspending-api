from django.db.models import F, Sum

from usaspending_api.spending.v2.views.object_class_explorer.recipient import recipient_awarding, recipient_funding


def program_activity_awarding(queryset, fiscal_year):
    # Program Activity Queryset
    pa = queryset.annotate(
        major_object_class_code=F('object_class__major_object_class'),
        awarding_agency_id=F('award__awarding_agency__id'),
        main_account_code=F('treasury_account__federal_account__main_account_code'),
        program_activity_name=F('program_activity__program_activity_name'),
        program_activity_code=F('program_activity__program_activity_code')
    ).values(
        'major_object_class_code', 'awarding_agency_id', 'main_account_code',
        'program_activity_name', 'program_activity_code').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    program_activity_total = pa.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in program_activity_total.items():
        program_activity_total = value

    # Unpack recipient results
    recipients_results = recipient_awarding(queryset, fiscal_year)

    program_activity_results = {
        'count': pa.count(),
        'total': program_activity_total,
        'end_date': fiscal_year,
        'program_activity': pa,
        'recipients': recipients_results
    }
    results = [
        program_activity_results
    ]
    return results


def program_activity_funding(queryset, fiscal_year):
    # Program Activity Queryset
    pa = queryset.annotate(
        major_object_class_code=F('object_class__major_object_class'),
        funding_agency_id=F('award__funding_agency__id'),
        main_account_code=F('treasury_account__federal_account__main_account_code'),
        program_activity_name=F('program_activity__program_activity_name'),
        program_activity_code=F('program_activity__program_activity_code')
    ).values(
        'major_object_class_code', 'funding_agency_id', 'main_account_code',
        'program_activity_name', 'program_activity_code').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    program_activity_total = pa.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in program_activity_total.items():
        program_activity_total = value

    # Unpack recipient results
    recipients_results = recipient_funding(queryset, fiscal_year)

    program_activity_results = {
        'count': pa.count(),
        'total': program_activity_total,
        'end_date': fiscal_year,
        'program_activity': pa,
        'recipients': recipients_results
    }
    results = [
        program_activity_results
    ]
    return results
