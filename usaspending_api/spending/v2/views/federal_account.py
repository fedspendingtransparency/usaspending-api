from django.db.models import F, Sum, CharField, Value

from usaspending_api.spending.v2.views.agency import awarding_top_tier_agency, awarding_sub_tier_agency
from usaspending_api.spending.v2.views.award import award_category, award
from usaspending_api.spending.v2.views.object_class import object_class_budget
from usaspending_api.spending.v2.views.program_activity import program_activity
from usaspending_api.spending.v2.views.recipient import recipient_budget


def federal_account_budget(queryset, fiscal_year):
    # Federal Account Queryset
    federal_accounts = queryset.annotate(
        id=F('financial_accounts_by_awards_id'),
        type=Value('federal_account', output_field=CharField()),
        name=F('treasury_account__federal_account__account_title'),
        code=F('treasury_account__federal_account__main_account_code'),
        amount=F('obligations_incurred_total_by_award_cpe')
    ).values(
        'id', 'type', 'name', 'code', 'amount').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    federal_accounts_total = federal_accounts.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in federal_accounts_total.items():
        federal_accounts_total = value

    # Unpack program activity results
    program_activity_results = program_activity(queryset, fiscal_year)
    # Unpack object class program activity results
    object_classes_results = object_class_budget(queryset, fiscal_year)
    # Unpack recipient results
    recipients_results = recipient_budget(queryset, fiscal_year)
    # Unpack award results
    award_category_results = award_category(queryset, fiscal_year)
    # Unpack awards
    awards_results = award(queryset, fiscal_year)
    # Unpack awarding agency
    awarding_top_tier_agencies_results = awarding_top_tier_agency(queryset, fiscal_year)
    # Unpack awarding sub tier results
    awarding_sub_tier_agencies_results = awarding_sub_tier_agency(queryset, fiscal_year)

    federal_accounts_results = {
        'total': federal_accounts_total,
        'end_date': fiscal_year,
        'results': federal_accounts
    }
    results = [
        federal_accounts_results,
        program_activity_results,
        object_classes_results,
        recipients_results,
        award_category_results,
        awards_results,
        awarding_top_tier_agencies_results,
        awarding_sub_tier_agencies_results
    ]
    return results
