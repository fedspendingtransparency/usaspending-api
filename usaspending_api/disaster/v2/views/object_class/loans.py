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


class ObjectClassLoansViewSet(LoansMixin, FabaOutlayMixin, LoansPaginationMixin, ElasticsearchAccountDisasterBase):
    """Provides insights on the Object Classes' loans from disaster/emergency funding per the requested filters"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/object_class/loans.md"
    agg_key = "financial_accounts_by_award.object_class"  # primary (tier-1) aggregation key
    nested_nonzero_fields = {"obligation": "transaction_obligated_amount", "outlay": "gross_outlay_amount_by_award_cpe"}
    nonzero_fields = {"outlay": "outlay_sum", "obligation": "obligated_sum"}
    query_fields = [
        "major_object_class_name",
        "major_object_class_name.contains",
        "object_class_name",
        "object_class_name.contains",
    ]
    top_hits_fields = [
        "financial_accounts_by_award.object_class_id",
        "financial_accounts_by_award.major_object_class_name",
        "financial_accounts_by_award.major_object_class",
        "financial_accounts_by_award.object_class_name",
        "financial_accounts_by_award.object_class",
    ]

    @cache_response()
    def post(self, request):
        self.filters.update({"award_type_codes": ["07", "08"]})
        self.has_children = True
        return self.perform_elasticsearch_search(loans=True)

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
        temp_results = {}
        child_results = []
        for bucket in info_buckets:
            child = self._build_child_json_result(bucket)
            child_results.append(child)
        for child in child_results:
            result = self._build_json_result(child)
            child.pop("parent_data")
            if result["id"] in temp_results.keys():
                temp_results[result["id"]] = {
                    "id": int(result["id"]),
                    "code": result["code"],
                    "description": result["description"],
                    "award_count": temp_results[result["id"]]["award_count"] + result["award_count"],
                    # the count of distinct awards contributing to the totals
                    "obligation": temp_results[result["id"]]["obligation"] + result["obligation"],
                    "outlay": temp_results[result["id"]]["outlay"] + result["outlay"],
                    "face_value_of_loan": bucket["count_awards_by_dim"]["sum_loan_value"]["value"],
                    "children": temp_results[result["id"]]["children"] + result["children"],
                }
            else:
                temp_results[result["id"]] = result
        results = [x for x in temp_results.values()]
        return results

    def _build_json_result(self, child):
        return {
            "id": child["parent_data"][1],
            "code": child["parent_data"][1],
            "description": child["parent_data"][0],
            "award_count": child["award_count"],
            # the count of distinct awards contributing to the totals
            "obligation": child["obligation"],
            "outlay": child["outlay"],
            "face_value_of_loan": child["face_value_of_loan"],
            "children": [child],
        }

    def _build_child_json_result(self, bucket: dict):
        return {
            "id": bucket["dim_metadata"]["hits"]["hits"][0]["_source"]["object_class_id"],
            "code": bucket["key"],
            "description": bucket["dim_metadata"]["hits"]["hits"][0]["_source"]["object_class_name"],
            # the count of distinct awards contributing to the totals
            "award_count": int(bucket["count_awards_by_dim"]["award_count"]["value"]),
            **{
                key: round(float(bucket.get(f"sum_{val}", {"value": 0})["value"]), 2)
                for key, val in self.nested_nonzero_fields.items()
            },
            "face_value_of_loan": bucket["count_awards_by_dim"]["sum_loan_value"]["value"],
            "parent_data": [
                bucket["dim_metadata"]["hits"]["hits"][0]["_source"]["major_object_class_name"],
                bucket["dim_metadata"]["hits"]["hits"][0]["_source"]["major_object_class"],
            ],
        }
