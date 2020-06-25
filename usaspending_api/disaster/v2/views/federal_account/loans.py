from django.db.models import Q, Sum, Count, F, Value, DecimalField
from django.db.models.functions import Coalesce
from rest_framework.response import Response

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    LoansPaginationMixin,
    LoansMixin,
)
from usaspending_api.disaster.v2.views.federal_account.spending import construct_response


class Loans(LoansMixin, LoansPaginationMixin, DisasterBase):
    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/federal_account/loans.md"

    @cache_response()
    def post(self, request):
        if self.pagination.sort_key == "face_value_of_loan":
            self.pagination.sort_key = "outlay"  # hack to re-use the Dataclasses
        results = construct_response(list(self.queryset), self.pagination)

        for result in results["results"]:
            for child in result["children"]:
                child.pop("obligation")
                child.pop("total_budgetary_resources")
                child["face_value_of_loan"] = child.pop("outlay")
            result.pop("obligation")
            result.pop("total_budgetary_resources")
            result["face_value_of_loan"] = result.pop("outlay")

        return Response(results)

    @property
    def queryset(self):
        filters = [
            Q(submission__reporting_period_start__gte=self.reporting_period_min),
            Q(
                Q(
                    Q(submission__reporting_period_end__lte=self.recent_monthly_submission["submission_reveal_date"])
                    & Q(submission__quarter_format_flag=False)
                )
                | Q(
                    Q(submission__reporting_period_end__lte=self.recent_quarterly_submission["submission_reveal_date"])
                    & Q(submission__quarter_format_flag=True)
                )
            ),
            Q(disaster_emergency_fund__in=self.def_codes),
            Q(award_id__isnull=False),
            Q(treasury_account__isnull=False),
            Q(treasury_account__federal_account__isnull=False),
        ]
        annotations = {
            "fa_code": F("treasury_account__federal_account__federal_account_code"),
            "count": Count("award_id", distinct=True),
            "description": F("treasury_account__account_title"),
            "code": F("treasury_account__tas_rendering_label"),
            "id": F("treasury_account__treasury_account_identifier"),
            "fa_description": F("treasury_account__federal_account__account_title"),
            "fa_id": F("treasury_account__federal_account_id"),
            "obligation": Value(0, DecimalField(max_digits=23, decimal_places=2)),  # Throw-away field
            "outlay": Coalesce(Sum("award__total_loan_value"), 0),
            "total_budgetary_resources": Value(None, DecimalField(max_digits=23, decimal_places=2)),  # Throw-away field
        }

        # Assuming it is more performant to fetch all rows once rather than
        #  run a count query and fetch only a page's worth of results
        return (
            FinancialAccountsByAwards.objects.filter(*filters)
            .values(
                "treasury_account__federal_account__id",
                "treasury_account__federal_account__federal_account_code",
                "treasury_account__federal_account__account_title",
            )
            .annotate(**annotations)
            .values(*annotations.keys())
        )
