import logging

from django.contrib.postgres.fields import ArrayField
from django.db.models import F, Value, IntegerField, Subquery, OuterRef
from django.views.decorators.csrf import csrf_exempt
from rest_framework.response import Response
from typing import List
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.json_helpers import json_str_to_dict
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    LoansPaginationMixin,
    LoansMixin,
    FabaOutlayMixin,
)
from usaspending_api.disaster.v2.views.elasticsearch_base import (
    ElasticsearchDisasterBase,
    ElasticsearchLoansPaginationMixin,
)
from usaspending_api.references.models import Agency, ToptierAgency
from usaspending_api.search.v2.elasticsearch_helper import get_summed_value_as_float

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


class LoansByAgencyViewSet(LoansPaginationMixin, LoansMixin, FabaOutlayMixin, DisasterBase):
    """
        This endpoint provides insights on the Agencies awarding loans from
        disaster/emergency funding per the requested filters.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/loans.md"

    @cache_response()
    def post(self, request):

        results = list(self.queryset.order_by(*self.pagination.robust_order_by_fields))
        return Response(
            {
                "totals": self.accumulate_total_values(results, include_loans=True),
                "results": results[self.pagination.lower_limit : self.pagination.upper_limit],
                "page_metadata": get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page),
            }
        )

    @property
    def queryset(self):

        query = self.construct_loan_queryset(
            "treasury_account__funding_toptier_agency_id", ToptierAgency, "toptier_agency_id"
        )

        annotations = {
            "id": Subquery(
                Agency.objects.filter(toptier_agency=OuterRef("toptier_agency_id"))
                .order_by("-toptier_flag", "id")
                .values("id")[:1]
            ),
            "code": F("toptier_code"),
            "description": F("name"),
            # Currently, this flavor of the endpoint can never have children
            "children": Value([], output_field=ArrayField(IntegerField())),
            "award_count": query.award_count_column,
            "obligation": query.obligation_column,
            "outlay": query.outlay_column,
            "face_value_of_loan": query.face_value_of_loan_column,
        }

        return query.queryset.annotate(**annotations).values(*annotations)


class LoansBySubtierAgencyViewSet(ElasticsearchLoansPaginationMixin, ElasticsearchDisasterBase):
    """
    This route takes DEF Codes and Award Type Codes and returns Loans by Subtier Agency, rolled up to include
    totals for each distinct Toptier agency.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/loans.md"

    required_filters = ["def_codes", "_loan_award_type_codes", "query"]
    query_fields = ["funding_toptier_agency_name.contains"]
    agg_key = "funding_toptier_agency_agg_key"  # primary (tier-1) aggregation key
    sub_agg_key = "funding_subtier_agency_agg_key"  # secondary (tier-2) sub-aggregation key

    def build_elasticsearch_result(self, info_buckets: List[dict]) -> List[dict]:
        results = []
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
        info = json_str_to_dict(bucket.get("key"))
        return {
            "id": int(info["id"]),
            "code": info["code"],
            "description": info["name"],
            # the count of distinct awards contributing to the totals
            "award_count": int(bucket.get("doc_count", 0)),
            **{
                column: get_summed_value_as_float(bucket, self.sum_column_mapping[column])
                for column in self.sum_column_mapping
            },
        }
