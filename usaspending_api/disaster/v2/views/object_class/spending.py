from typing import List

from django.db.models import Q, Sum, F, Value, Case, When, Min, TextField, IntegerField
from django.db.models.functions import Coalesce, Cast
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.v2.views.elasticsearch_account_base import ElasticsearchAccountDisasterBase
from usaspending_api.disaster.v2.views.object_class.object_class_result import (
    ObjectClassResults,
    MajorClass,
    ObjectClass,
)
from usaspending_api.disaster.v2.views.disaster_base import (
    PaginationMixin,
    SpendingMixin,
    FabaOutlayMixin,
)
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.search.v2.elasticsearch_helper import get_summed_value_as_float


def construct_response(results: list, pagination: Pagination, strip_total_budgetary_resources=True):
    object_classes = ObjectClassResults()
    for row in results:
        major_code = row.pop("major_code")
        major_class = MajorClass(
            id=major_code, code=major_code, award_count=0, description=row.pop("major_description")
        )
        object_classes[major_class].include(ObjectClass(**row))

    return {
        "results": object_classes.finalize(pagination, strip_total_budgetary_resources),
        "page_metadata": get_pagination_metadata(len(object_classes), pagination.limit, pagination.page),
    }


class ObjectClassSpendingViewSet(SpendingMixin, FabaOutlayMixin, PaginationMixin, ElasticsearchAccountDisasterBase):
    """View to implement the API"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/object_class/spending.md"

    # Defined for the Elasticsearch implementation of Spending by Award
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
        if self.spending_type == "award":
            response = self.perform_elasticsearch_search()
        else:
            results = list(self.total_queryset)
            response = construct_response(results, self.pagination)
            response["totals"] = self.accumulate_total_values(results)

        return Response(response)

    @property
    def total_queryset(self):
        filters = [
            self.is_in_provided_def_codes,
            self.is_non_zero_total_spending,
            self.all_closed_defc_submissions,
            Q(object_class__isnull=False),
        ]

        object_class_annotations = {
            "major_code": F("object_class__major_object_class"),
            "description": F("object_class__object_class_name"),
            "code": F("object_class__object_class"),
            "id": Cast(Min("object_class_id"), TextField()),
            "major_description": F("object_class__major_object_class_name"),
        }

        annotations = {
            **object_class_annotations,
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
            "award_count": Value(None, output_field=IntegerField()),
        }

        # Assuming it is more performant to fetch all rows once rather than
        #  run a count query and fetch only a page's worth of results
        return (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters)
            .values("object_class__major_object_class", "object_class__major_object_class_name",)
            .annotate(**annotations)
            .values(*annotations.keys())
        )

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
        }
