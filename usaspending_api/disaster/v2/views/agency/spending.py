import json
import logging
from decimal import Decimal
from typing import List

from django.contrib.postgres.fields import ArrayField
from django.db.models import Case, DecimalField, F, IntegerField, Q, Sum, Value, When, Subquery, OuterRef, Func
from django.db.models.functions import Coalesce
from django.views.decorators.csrf import csrf_exempt
from django_cte import With
from rest_framework.response import Response

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    PaginationMixin,
    SpendingMixin,
    FabaOutlayMixin,
)
from usaspending_api.disaster.v2.views.elasticsearch_base import (
    ElasticsearchDisasterBase,
    ElasticsearchSpendingPaginationMixin,
)
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import GTASSF133Balances, Agency, ToptierAgency


logger = logging.getLogger(__name__)


def route_agency_spending_backend(**initkwargs):
    """
    Per API contract, delegate requests that specify `award_type_codes` to the Elasticsearch-backend that gets sum
    amounts based on subtier Agency associated with the linked award.
    Otherwise use the Postgres-backend that gets sum amount from toptier Agency associated with the File C TAS
    """
    spending_by_subtier_agency = SpendingBySubtierAgencyViewSet.as_view(**initkwargs)
    spending_by_agency = SpendingByAgencyViewSet.as_view(**initkwargs)

    @csrf_exempt
    def route_agency_spending_backend(request, *args, **kwargs):
        """
        Returns disaster spending by agency.  If agency type codes are provided, the characteristics of
        the result are modified a bit.  Instead of being purely a rollup of File C agency loans, the results
        become a rollup of File D subtier agencies by toptier agency and subtiers will be included as children
        of the toptier agency.
        """
        if DisasterBase.requests_award_type_codes(request) & DisasterBase.requests_award_spending_type(request):
            return spending_by_subtier_agency(request, *args, **kwargs)
        return spending_by_agency(request, *args, **kwargs)

    route_agency_spending_backend.endpoint_doc = SpendingBySubtierAgencyViewSet.endpoint_doc
    route_agency_spending_backend.__doc__ = SpendingBySubtierAgencyViewSet.__doc__
    return route_agency_spending_backend


class SpendingByAgencyViewSet(PaginationMixin, SpendingMixin, FabaOutlayMixin, DisasterBase):
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

        cte_filters = [
            Q(treasury_account__isnull=False),
            self.all_closed_defc_submissions,
            self.is_in_provided_def_codes,
            self.is_non_zero_total_spending,
        ]

        cte_annotations = {
            "funding_toptier_agency_id": F("treasury_account__funding_toptier_agency_id"),
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
        }

        cte = With(
            FinancialAccountsByProgramActivityObjectClass.objects.filter(*cte_filters)
            .values("treasury_account__funding_toptier_agency_id")
            .annotate(**cte_annotations)
            .values(*cte_annotations)
        )

        annotations = {
            "id": Subquery(
                Agency.objects.filter(toptier_agency_id=OuterRef("toptier_agency_id"))
                .order_by("-toptier_flag", "id")
                .values("id")[:1]
            ),
            "code": F("toptier_code"),
            "description": F("name"),
            # Currently, this endpoint can never have children w/o type = `award` & `award_type_codes`
            "children": Value([], output_field=ArrayField(IntegerField())),
            "award_count": Value(0, output_field=IntegerField()),
            "obligation": cte.col.obligation,
            "outlay": cte.col.outlay,
            "total_budgetary_resources": Coalesce(
                Subquery(
                    GTASSF133Balances.objects.filter(
                        disaster_emergency_fund_code__in=self.def_codes,
                        fiscal_period=self.latest_reporting_period["submission_fiscal_month"],
                        fiscal_year=self.latest_reporting_period["submission_fiscal_year"],
                        treasury_account_identifier__funding_toptier_agency_id=OuterRef("toptier_agency_id"),
                    )
                    .annotate(amount=Func("budget_authority_appropriation_amount_cpe", function="Sum"))
                    .values("amount"),
                    output_field=DecimalField(),
                ),
                0,
            ),
        }

        return (
            cte.join(ToptierAgency, toptier_agency_id=cte.col.funding_toptier_agency_id)
            .with_cte(cte)
            .annotate(**annotations)
            .values(*annotations)
        )

    @property
    def award_queryset(self):
        filters = [
            self.is_in_provided_def_codes,
            self.all_closed_defc_submissions,
            Q(treasury_account__isnull=False),
            Q(treasury_account__funding_toptier_agency__isnull=False),
        ]

        annotations = {
            "id": Subquery(
                Agency.objects.filter(toptier_agency=OuterRef("treasury_account__funding_toptier_agency"))
                .order_by("-toptier_flag", "id")
                .values("id")[:1]
            ),
            "code": F("treasury_account__funding_toptier_agency__toptier_code"),
            "description": F("treasury_account__funding_toptier_agency__name"),
            # Currently, this endpoint can never have children.
            "children": Value([], output_field=ArrayField(IntegerField())),
            "award_count": self.unique_file_c_count(),
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
    agg_key = "funding_toptier_agency_agg_key"  # primary (tier-1) aggregation key
    sub_agg_key = "funding_subtier_agency_agg_key"  # secondary (tier-2) sub-aggregation key

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        results = []
        info_buckets = response.get(self.agg_group_name, {}).get("buckets", [])
        for bucket in info_buckets:
            result = self._build_json_result(bucket)
            child_info_buckets = bucket.get(self.sub_agg_group_name, {}).get("buckets", [])
            children = []
            for child_bucket in child_info_buckets:
                children.append(self._build_json_result(child_bucket))
            result["children"] = children
            results.append(result)

        return results

    def _build_json_result(self, bucket: dict):
        info = json.loads(bucket.get("key"))
        return {
            "id": int(info["id"]),
            "code": info["code"],
            "description": info["name"],
            # the count of distinct subtier agencies contributing to the totals
            "award_count": int(bucket.get("doc_count", 0)),
            **{
                column: int(bucket.get(self.sum_column_mapping[column], {"value": 0})["value"]) / Decimal("100")
                for column in self.sum_column_mapping
            },
        }
