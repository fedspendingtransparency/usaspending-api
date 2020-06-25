from django.db.models import Q, Sum, Count, F, Value, DecimalField, Case, When, OuterRef, Exists
from django.db.models.functions import Coalesce
from rest_framework.response import Response
from rest_framework.request import Request


from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase
from usaspending_api.search.models import LoanAwardSearchMatview
from usaspending_api.common.validator import TinyShield
from usaspending_api.awards.models import FinancialAccountsByAwards


class SpendingByGeographyViewSet(DisasterBase):
    """Spending by Recipient Location"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/spending_by_geography.md"

    @cache_response()
    def post(self, request: Request) -> Response:

        models = [
            {
                "key": "geo_layer_filters",
                "name": "geo_layer_filters",
                "type": "array",
                "array_type": "text",
                "text_type": "search",
                "allow_nulls": False,
                "optional": False,
            },
            {
                "key": "geo_layer",
                "name": "geo_layer",
                "type": "enum",
                "enum_values": ["state", "county", "district"],
                "text_type": "search",
                "allow_nulls": False,
                "optional": True,
            },
            {
                "key": "spending_type",
                "name": "spending_type",
                "type": "enum",
                "enum_values": ["per-capita", "award", "loan"],
                "allow_nulls": False,
                "optional": False,
            },
        ]

        self.filters.update(TinyShield(models).block(self.request.data))

        if self.filters["spending_type"] == "loan":
            queryset = self.loan_queryset
            from usaspending_api.common.helpers.orm_helpers import generate_raw_quoted_query
            print(f"=======================================\nQueryset: {generate_raw_quoted_query(queryset)}")
            results = list(queryset)
            return Response({"results": results, "url": self.endpoint_doc, "filters": self.filters})
        else:
            raise NotImplementedError

    @property
    def loan_queryset(self):
        filters = [
            Q(award_id=OuterRef("award_id")),
            Q(submission__reporting_period_start__gte=self.reporting_period_min),
            Q(
                Q(
                    submission__quarter_format_flag=True,
                    submission__reporting_fiscal_year__lte=self.last_closed_quarterly_submission_dates[
                        "submission_fiscal_year"
                    ],
                    submission__reporting_fiscal_quarter__lte=self.last_closed_quarterly_submission_dates[
                        "submission_fiscal_quarter"
                    ],
                )
                | Q(
                    submission__quarter_format_flag=False,
                    submission__reporting_fiscal_year__lte=self.last_closed_monthly_submission_dates[
                        "submission_fiscal_year"
                    ],
                    submission__reporting_fiscal_period__lte=self.last_closed_monthly_submission_dates[
                        "submission_fiscal_month"
                    ],
                )
            ),
            Q(disaster_emergency_fund__in=self.def_codes),
            Q(award_id__isnull=False),
        ]

        annotations = {
            "face_value_loan": Coalesce(Sum("face_value_loan_guarantee"), 0),
            "district": F("recipient_location_congressional_code"),
            "county": F("recipient_location_county_code"),
            "state": F("recipient_location_state_code"),
        }
        # fields = [
        #     "recipient_location_congressional_code",
        #     "recipient_location_country_code",
        #     "recipient_location_state_code",
        #     "recipient_location_county_code",
        #     "recipient_location_county_name",
        # ]

        return (
            LoanAwardSearchMatview.objects
            # .annotate(
            #     include=Exists(FinancialAccountsByAwards.objects.filter(*filters).values("award_id"))
            # )
            .filter(recipient_location_country_code="USA")
            .values(
                "recipient_location_congressional_code",
                "recipient_location_county_code",
                "recipient_location_state_code",
            )
            .annotate(**annotations)
            .values(*annotations.keys())
        )
