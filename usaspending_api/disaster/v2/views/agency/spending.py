import json
from decimal import Decimal
from typing import List

from django.contrib.postgres.fields import ArrayField
from django.db.models import Case, DecimalField, F, IntegerField, Q, Sum, Value, When
from django.db.models.functions import Coalesce
from rest_framework.request import Request
from rest_framework.response import Response
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase, PaginationMixin, SpendingMixin, \
    ElasticsearchSpendingPaginationMixin
from usaspending_api.disaster.v2.views.elasticsearch_base import ElasticsearchDisasterBase
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


def route_agency_spending_backend(request: Request) -> bool:
    """
    Per API contract, delegate requests that specify `award_type_codes` to the Elasticsearched-backend that gets sum
    amounts based on subtier Agency associated with the linked award.
    Otherwise use the Postgres-backend that gets sum amount from toptier Agency associated with the File C TAS
    """
    if request and request.data and "filter" in request.data and "award_type_codes" in request.data["filter"]:
        return SpendingBySubtierAgencyViewSet.as_view()
    return SpendingByAgencyViewSet.as_view()


route_agency_spending_backend.endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/spending.md"


class SpendingByAgencyViewSet(PaginationMixin, SpendingMixin, DisasterBase):
    """ Returns disaster spending by agency. """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/spending.md"

    @cache_response()
    def post(self, request):
        if self.spending_type == "award":
            results = self.award_queryset
        else:
            results = self.total_queryset

        return Response(
            {
                "results": results.order_by(self.pagination.order_by)[
                    self.pagination.lower_limit : self.pagination.upper_limit
                ],
                "page_metadata": get_pagination_metadata(results.count(), self.pagination.limit, self.pagination.page),
            }
        )

    @property
    def total_queryset(self):
        filters = [
            Q(
                Q(obligations_incurred_by_program_object_class_cpe__gt=0)
                | Q(obligations_incurred_by_program_object_class_cpe__lt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__gt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__lt=0)
            ),
            Q(disaster_emergency_fund__in=self.def_codes),
            Q(treasury_account__isnull=False),
            Q(treasury_account__funding_toptier_agency__isnull=False),
            self.all_closed_defc_submissions,
        ]

        annotations = {
            "id": F("treasury_account__funding_toptier_agency"),
            "code": F("treasury_account__funding_toptier_agency__toptier_code"),
            "description": F("treasury_account__funding_toptier_agency__name"),
            # Currently, this endpoint can never have children.
            "children": Value([], output_field=ArrayField(IntegerField())),
            "count": Value(0, output_field=IntegerField()),
            "obligation": Coalesce(
                Sum(
                    Case(
                        When(
                            self.final_period_submission_query_filters,
                            then=F("obligations_incurred_by_program_object_class_cpe"),
                        ),
                        default=Value(0),
                    )
                ),
                0,
            ),
            "outlay": Coalesce(
                Sum(
                    Case(
                        When(
                            self.final_period_submission_query_filters,
                            then=F("gross_outlay_amount_by_program_object_class_cpe"),
                        ),
                        default=Value(0),
                    )
                ),
                0,
            ),
            "total_budgetary_resources": Coalesce(
                Sum("treasury_account__gtas__budget_authority_appropriation_amount_cpe"), 0
            ),
        }

        return (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters)
            .values(
                "treasury_account__funding_toptier_agency",
                "treasury_account__funding_toptier_agency__toptier_code",
                "treasury_account__funding_toptier_agency__name",
            )
            .annotate(**annotations)
            .values(*annotations.keys())
        )

    @property
    def award_queryset(self):
        filters = [
            Q(disaster_emergency_fund__in=self.def_codes),
            Q(treasury_account__isnull=False),
            Q(treasury_account__funding_toptier_agency__isnull=False),
            self.all_closed_defc_submissions,
        ]

        annotations = {
            "id": F("treasury_account__funding_toptier_agency"),
            "code": F("treasury_account__funding_toptier_agency__toptier_code"),
            "description": F("treasury_account__funding_toptier_agency__name"),
            # Currently, this endpoint can never have children.
            "children": Value([], output_field=ArrayField(IntegerField())),
            "count": Value(0, output_field=IntegerField()),
            "obligation": Coalesce(Sum("transaction_obligated_amount"), 0),
            "outlay": Coalesce(
                Sum(
                    Case(
                        When(self.final_period_submission_query_filters, then=F("gross_outlay_amount_by_award_cpe")),
                        default=Value(0),
                    )
                ),
                0,
            ),
            "total_budgetary_resources": Value(None, DecimalField()),  # NULL for award spending
        }

        return (
            FinancialAccountsByAwards.objects.filter(*filters)
            .values(
                "treasury_account__funding_toptier_agency",
                "treasury_account__funding_toptier_agency__toptier_code",
                "treasury_account__funding_toptier_agency__name",
            )
            .annotate(**annotations)
            .values(*annotations.keys())
        )


class SpendingBySubtierAgencyViewSet(ElasticsearchSpendingPaginationMixin, ElasticsearchDisasterBase):
    """
    This route takes DEF Codes and Award Type Codes and returns Spending by Subtier Agency, rolled up to include
    totals for each distinct Toptier agency.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/spending.md"

    required_filters = ["def_codes", "award_type_codes", "query"]
    query_fields = ["funding_toptier_agency_name"]
    agg_key = "funding_subtier_agency_agg_key"

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        results = []
        info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])
        for bucket in info_buckets:
            info = json.loads(bucket.get("key"))
            results.append(
                {
                    "id": int(info["id"]),
                    "code": info["code"],
                    "description": info["name"],
                    "count": int(bucket.get("doc_count", 0)),
                    **{
                        column: int(bucket.get(self.sum_column_mapping[column], {"value": 0})["value"]) / Decimal("100")
                        for column in self.sum_column_mapping
                    },
                }
            )

        return results
