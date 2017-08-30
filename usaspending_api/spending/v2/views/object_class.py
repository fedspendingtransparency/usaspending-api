from django.db.models import F, Sum, CharField, Value

from usaspending_api.spending.v2.views.agency import awarding_top_tier_agency, awarding_sub_tier_agency
from usaspending_api.spending.v2.views.award import award_category, award
from usaspending_api.spending.v2.views.recipient import recipient_budget


def object_class_budget(queryset, fiscal_year):
    # Object Classes Queryset
    object_classes = queryset.annotate(
        id=F('financial_accounts_by_awards_id'),
        type=Value('object_class', output_field=CharField()),
        name=F('object_class__major_object_class_name'),
        code=F('object_class__major_object_class'),
        amount=F('obligations_incurred_total_by_award_cpe')
    ).values(
        'id', 'type', 'name', 'code', 'amount').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    object_classes_total = object_classes.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in object_classes_total.items():
        object_classes_total = value

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

    object_classes_results = {
        'total': object_classes_total,
        'end_date': fiscal_year,
        'results': object_classes
    }
    results = [
        object_classes_results,
        recipients_results,
        award_category_results,
        awards_results,
        awarding_top_tier_agencies_results,
        awarding_sub_tier_agencies_results
    ]
    return results
