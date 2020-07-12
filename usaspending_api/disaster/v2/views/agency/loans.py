import json
import logging
from decimal import Decimal
from typing import List

from django.contrib.postgres.fields import ArrayField
from django.db.models import Q, Sum, F, Value, Case, When, IntegerField
from django.db.models.functions import Coalesce
from django.views.decorators.csrf import csrf_exempt
from rest_framework.response import Response
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.awards.v2.lookups.lookups import loan_type_mapping
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    LoansPaginationMixin,
    LoansMixin,
)
from usaspending_api.disaster.v2.views.elasticsearch_base import (
    ElasticsearchDisasterBase,
    ElasticsearchLoansPaginationMixin,
)


logger = logging.getLogger(__name__)


def route_agency_loans_backend(**initkwargs):
    """
    Per API contract, delegate requests that specify `award_type_codes` to the Elasticsearch-backend that gets sum
    amounts based on subtier Agency associated with the linked award.
    Otherwise use the Postgres-backend that gets sum amount from toptier Agency associated with the File C TAS
    """
    loans_by_subtier_agency = LoansBySubtierAgencyViewSet.as_view(**initkwargs)
    loans_by_agency = LoansByAgencyViewSet.as_view(**initkwargs)

    @csrf_exempt
    def route_agency_loans_backend(request, *args, **kwargs):
        if DisasterBase.requests_award_type_codes(request):
            return loans_by_subtier_agency(request, *args, **kwargs)
        return loans_by_agency(request, *args, **kwargs)

    route_agency_loans_backend.endpoint_doc = LoansBySubtierAgencyViewSet.endpoint_doc
    route_agency_loans_backend.__doc__ = LoansBySubtierAgencyViewSet.__doc__
    return route_agency_loans_backend


class LoansByAgencyViewSet(LoansPaginationMixin, LoansMixin, DisasterBase):
    """ Returns loan disaster spending by agency. """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/loans.md"

    @cache_response()
    def post(self, request):
        results = self.queryset
        return Response(
            {
                "results": results.order_by(self.pagination.order_by)[
                    self.pagination.lower_limit : self.pagination.upper_limit
                ],
                "page_metadata": get_pagination_metadata(results.count(), self.pagination.limit, self.pagination.page),
            }
        )

    @property
    def queryset(self):
        filters = [
            Q(award__type__in=loan_type_mapping),
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
            "face_value_of_loan": Coalesce(Sum("award__total_loan_value"), 0),
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


class LoansBySubtierAgencyViewSet(ElasticsearchLoansPaginationMixin, ElasticsearchDisasterBase):
    """
    This route takes DEF Codes and Award Type Codes and returns Loans by Subtier Agency, rolled up to include
    totals for each distinct Toptier agency.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/loans.md"

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
            "count": int(bucket.get("doc_count", 0)),
            **{
                column: int(bucket.get(self.sum_column_mapping[column], {"value": 0})["value"]) / Decimal("100")
                for column in self.sum_column_mapping
            },
        }
