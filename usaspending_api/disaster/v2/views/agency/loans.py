import logging
from decimal import Decimal
from typing import List

from django.contrib.postgres.fields import ArrayField
from django.db.models import F, IntegerField, OuterRef, Subquery, Value
from django.views.decorators.csrf import csrf_exempt

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    FabaOutlayMixin,
    LoansMixin,
    LoansPaginationMixin,
)
from usaspending_api.disaster.v2.views.elasticsearch_account_base import ElasticsearchAccountDisasterBase
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


class LoansByAgencyViewSet(LoansPaginationMixin, ElasticsearchAccountDisasterBase, LoansMixin, FabaOutlayMixin):
    """
    This endpoint provides insights on the Agencies awarding loans from
    disaster/emergency funding per the requested filters.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/loans.md"
    required_filters = ["def_codes", "query"]
    query_fields = ["funding_toptier_agency_name.contains"]
    agg_key = "funding_toptier_agency_id"  # primary (tier-1) aggregation key
    nonzero_fields = {"obligation": "transaction_obligated_amount", "outlay": "gross_outlay_amount_by_award_cpe"}
    top_hits_fields = [
        "funding_toptier_agency_code",
        "funding_toptier_agency_name",
    ]

    @cache_response()
    def post(self, request):
        self.filters.update({"award_type_codes": ["07", "08"]})
        return self.perform_elasticsearch_search(loans=True)

    def build_elasticsearch_result(self, info_buckets: List[dict]) -> List[dict]:
        results = []
        for bucket in info_buckets:
            results.append(self._build_json_result(bucket))
        return results

    def _build_json_result(self, bucket: dict):
        return {
            "id": int(bucket["key"]),
            "code": bucket["dim_metadata"]["hits"]["hits"][0]["_source"]["funding_toptier_agency_code"],
            "description": bucket["dim_metadata"]["hits"]["hits"][0]["_source"]["funding_toptier_agency_name"],
            "children": [],
            # the count of distinct awards contributing to the totals
            "award_count": len(bucket["group_by_awards"]["buckets"]),
            **{
                key: Decimal(bucket.get(f"sum_{val}", {"value": 0})["value"])
                for key, val in self.nonzero_fields.items()
            },
            # Sum all of the loan values together and exclude the `None` values
            "face_value_of_loan": sum(
                [
                    float(award["award_metadata"]["hits"]["hits"][0]["_source"]["total_loan_value"])
                    for award in bucket["group_by_awards"]["buckets"]
                    if award["award_metadata"]["hits"]["hits"][0]["_source"]["total_loan_value"] is not None
                ],
            ),
        }

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
    top_hits_fields = ["funding_toptier_agency_name", "funding_toptier_agency_code", "funding_agency_id"]
    sub_top_hits_fields = ["funding_subtier_agency_name", "funding_subtier_agency_code", "funding_agency_id"]

    def build_elasticsearch_result(self, info_buckets: List[dict]) -> List[dict]:
        results = []
        for bucket in info_buckets:
            result = self._build_json_result(bucket, child=False)
            child_info_buckets = bucket.get(self.sub_agg_group_name, {}).get("buckets", [])
            children = []
            for child_bucket in child_info_buckets:
                children.append(self._build_json_result(child_bucket, child=True))
            result["children"] = children
            results.append(result)

        return results

    def _build_json_result(self, bucket: dict, child: bool):
        agency_id = None
        tier = "sub" if child else "top"
        tid = Agency.objects.filter(
            id=bucket["dim_metadata"]["hits"]["hits"][0]["_source"]["funding_agency_id"]
        ).first()
        if tid:
            toptier_id = tid.toptier_agency_id
            aid = Agency.objects.filter(toptier_agency_id=toptier_id).order_by("-toptier_flag", "-id").first()
            if aid:
                agency_id = aid.id
        info = bucket.get("key")
        return {
            "id": agency_id,
            "code": info,
            "description": bucket["dim_metadata"]["hits"]["hits"][0]["_source"][f"funding_{tier}tier_agency_name"],
            # the count of distinct awards contributing to the totals
            "award_count": int(bucket.get("doc_count", 0)),
            **{
                column: get_summed_value_as_float(
                    (
                        bucket.get("nested", {}).get("filtered_aggs", {})
                        if column != "face_value_of_loan"
                        else bucket.get("nested", {}).get("filtered_aggs", {}).get("reverse_nested")
                    ),
                    self.sum_column_mapping[column],
                )
                for column in self.sum_column_mapping
            },
        }
