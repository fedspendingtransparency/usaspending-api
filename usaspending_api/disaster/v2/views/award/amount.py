from django.db.models import Q, Sum, Count, F, Value, Case, When
from django.db.models.functions import Coalesce
from rest_framework.response import Response

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase, AwardTypeMixin


def submission_window_cutoff(min_date, monthly_sub, quarterly_sub):
    sub_queries = []
    if monthly_sub:
        sub_queries.append(
            Q(
                Q(submission__quarter_format_flag=False)
                & Q(submission__reporting_period_end__lte=monthly_sub["submission_reveal_date"])
            )
        )

    sub_queries.append(
        Q(
            Q(submission__quarter_format_flag=True)
            & Q(submission__reporting_period_end__lte=quarterly_sub["submission_reveal_date"])
        )
    )

    sub_queryset = sub_queries.pop()
    for query in sub_queries:
        sub_queryset |= query

    return [
        Q(submission__reporting_period_start__gte=min_date),
        Q(sub_queryset),
    ]


class AmountViewSet(AwardTypeMixin, DisasterBase):
    """View to implement the API"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/award/amount.md"

    @cache_response()
    def post(self, request):
        return Response(self.queryset)

    @property
    def queryset(self):
        filters = [
            Q(award__type__in=self.award_type_codes),
            Q(award__isnull=False),
            Q(disaster_emergency_fund__in=self.def_codes),
            Q(treasury_account__federal_account__isnull=False),
            Q(treasury_account__isnull=False),
        ]
        filters.extend(
            submission_window_cutoff(
                self.reporting_period_min,
                self.last_closed_monthly_submission_dates,
                self.last_closed_quarterly_submission_dates,
            )
        )
        case_when_queries = []
        if self.last_closed_monthly_submission_dates:
            case_when_queries.append(
                Q(
                    submission__reporting_fiscal_year=self.last_closed_monthly_submission_dates[
                        "submission_fiscal_year"
                    ],
                    submission__reporting_fiscal_period=self.last_closed_monthly_submission_dates[
                        "submission_fiscal_month"
                    ],
                    submission__quarter_format_flag=False,
                )
            )

        case_when_queries.append(
            Q(
                submission__reporting_fiscal_year=self.last_closed_quarterly_submission_dates["submission_fiscal_year"],
                submission__reporting_fiscal_quarter=self.last_closed_quarterly_submission_dates[
                    "submission_fiscal_quarter"
                ],
                submission__quarter_format_flag=True,
            )
        )

        case_when_query = case_when_queries.pop()
        for query in case_when_queries:
            case_when_query |= query

        fields = {
            "count": Count("award_id", distinct=True),
            "obligation": Coalesce(Sum("transaction_obligated_amount"), 0),
            "outlay": Coalesce(
                Sum(Case(When(case_when_query, then=F("gross_outlay_amount_by_award_cpe")), default=Value(0),)), 0
            ),
        }

        return FinancialAccountsByAwards.objects.filter(*filters).aggregate(**fields)
