from typing import List

from django.db.models import F, Value, TextField, Min
from django.db.models.functions import Cast
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.orm_helpers import ConcatAll
from usaspending_api.disaster.v2.views.disaster_base import (
    LoansPaginationMixin,
    LoansMixin,
    FabaOutlayMixin,
)
from usaspending_api.disaster.v2.views.elasticsearch_account_base import ElasticsearchAccountDisasterBase
from usaspending_api.references.models import ObjectClass
from usaspending_api.search.v2.elasticsearch_helper import get_summed_value_as_float


class ObjectClassLoansViewSet(LoansMixin, FabaOutlayMixin, LoansPaginationMixin, ElasticsearchAccountDisasterBase):
    """Provides insights on the Object Classes' loans from disaster/emergency funding per the requested filters"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/object_class/loans.md"
    agg_key = "financial_accounts_by_award.major_object_class"  # primary (tier-1) aggregation key
    nested_nonzero_fields = {"outlay": "gross_outlay_amount_by_award_cpe", "obligation": "transaction_obligated_amount"}
    query_fields = [
        "major_object_class",
        "major_object_class_name.contains",
        "object_class",
        "object_class_name.contains",
    ]
    sub_agg_key = "financial_accounts_by_award.object_class"
    sub_top_hits_fields = [
        "financial_accounts_by_award.object_class_id",
        "financial_accounts_by_award.object_class_name",
    ]
    top_hits_fields = [
        "financial_accounts_by_award.object_class_id",
        "financial_accounts_by_award.major_object_class_name",
    ]

    @cache_response()
    def post(self, request):
        self.filters.update({"award_type": ["07", "08"]})
        return self.perform_elasticsearch_search()
        # rename hack to use the Dataclasses, setting to Dataclass attribute name
        # if self.pagination.sort_key == "face_value_of_loan":
        #     self.pagination.sort_key = "total_budgetary_resources"
        #
        # results = list(self.queryset)
        # results = [{("id" if k == "id_" else k): v for k, v in r.items()} for r in results]
        # results = construct_response(results, self.pagination, False)
        #
        # # rename hack to use the Dataclasses, swapping back in desired loan field name
        # for result in results["results"]:
        #     for child in result["children"]:
        #         child["face_value_of_loan"] = child.pop("total_budgetary_resources")
        #     result["face_value_of_loan"] = result.pop("total_budgetary_resources")
        #
        # results["totals"] = self.accumulate_total_values(results["results"], ["award_count", "face_value_of_loan"])
        #
        # return Response(results)

    @property
    def queryset(self):
        query = self.construct_loan_queryset(
            ConcatAll("object_class__major_object_class", Value(":"), "object_class__object_class"),
            ObjectClass.objects.annotate(join_key=ConcatAll("major_object_class", Value(":"), "object_class")),
            "join_key",
        )

        annotations = {
            "major_code": F("major_object_class"),
            "description": Min("object_class_name"),
            "code": F("object_class"),
            "id_": Cast(Min("id"), output_field=TextField()),
            "major_description": Min("major_object_class_name"),
            "obligation": query.obligation_column,
            "outlay": query.outlay_column,
            "total_budgetary_resources": query.face_value_of_loan_column,
            "award_count": query.award_count_column,
        }

        return query.queryset.values("major_object_class", "object_class").annotate(**annotations).values(*annotations)

    def build_elasticsearch_result(self, info_buckets: List[dict]) -> List[dict]:
        results = []

        for bucket in info_buckets:
            result = self._build_json_result(bucket, True)
            child_info_buckets = bucket.get(self.sub_agg_group_name, {}).get("buckets", [])
            children = []
            for child_bucket in child_info_buckets:
                children.append(self._build_json_result(child_bucket, False))
            result["children"] = children
            results.append(result)

        return results

    def _build_json_result(self, bucket: dict, is_parent: bool):
        if is_parent:
            description_key = "major_object_class_name"
        else:
            description_key = "object_class_name"

        return {
            "id": bucket["dim_metadata"]["hits"]["hits"][0]["_source"]["object_class_id"],
            "code": bucket["key"],
            "description": bucket["dim_metadata"]["hits"]["hits"][0]["_source"][description_key],
            # the count of distinct awards contributing to the totals
            "award_count": int(bucket["count_awards_by_dim"]["award_count"]["value"]),
            **{key: get_summed_value_as_float(bucket, f"sum_{val}") for key, val in self.nested_nonzero_fields.items()},
            "face_value_of_loan": bucket["count_awards_by_dim"]["sum_loan_value"]["value"],
        }
