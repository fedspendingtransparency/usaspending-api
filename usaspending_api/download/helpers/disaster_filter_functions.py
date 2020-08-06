from typing import List

from django_cte import With
from django.db.models import Q, Sum, Case, When, Value, F, Count
from django.db.models.functions import Coalesce


from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.disaster.v2.views.disaster_base import (
    filter_by_defc_closed_periods,
    filter_by_latest_closed_periods,
)
from usaspending_api.search.models import AwardSearchView


def _disaster_recipient_aggregations() -> dict:
    return {
        "award_obligations": Coalesce(Sum("total_obligation_by_award"), 0),
        "award_outlays": Coalesce(Sum("total_outlay_by_award"), 0),
        "face_value_of_loans": Coalesce(Sum("total_loan_value"), 0),
        "number_of_awards": Count("award_id", distinct=True),
    }


def disaster_filter_function(filters: dict, download_type: str, values: List[str]):
    aggregation_mapping = {"disaster_recipient": _disaster_recipient_aggregations}

    def_codes = filters["def_codes"]
    query = filters.get("query")
    award_type_codes = filters.get("award_type_codes")

    award_filters = [~Q(total_loan_value=0) | ~Q(total_obligation_by_award=0) | ~Q(total_outlay_by_award=0)]
    if query:
        query_text = query["text"]
        q = Q()
        for field in query["fields"]:
            q |= Q(**{field: query_text})
        award_filters.append(q)
    if award_type_codes:
        award_filters.append(Q(type__in=award_type_codes))

    faba_filters = [filter_by_defc_closed_periods(), Q(disaster_emergency_fund__code__in=def_codes)]

    dollar_annotations = {
        "inner_obligation": Coalesce(Sum("transaction_obligated_amount"), 0),
        "inner_outlay": Coalesce(
            Sum(
                Case(
                    When(filter_by_latest_closed_periods(), then=F("gross_outlay_amount_by_award_cpe")),
                    default=Value(0),
                )
            ),
            0,
        ),
    }

    cte = With(
        FinancialAccountsByAwards.objects.filter(*faba_filters)
        .values("award_id")
        .annotate(**dollar_annotations)
        .exclude(inner_obligation=0, inner_outlay=0)
    )

    return (
        cte.join(AwardSearchView, award_id=cte.col.award_id)
        .with_cte(cte)
        .annotate(total_obligation_by_award=cte.col.inner_obligation, total_outlay_by_award=cte.col.inner_outlay)
        .filter(*award_filters)
        .values(*values)
        .annotate(**aggregation_mapping[download_type]())
    )
