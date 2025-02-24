from decimal import Decimal
from typing import List

from django.db.models import F, IntegerField, Min, Q, QuerySet, Sum, TextField, Value
from django.db.models.functions import Cast
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.calculations.file_b import FileBCalculations
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    FabaOutlayMixin,
    PaginationMixin,
    SpendingMixin,
)
from usaspending_api.disaster.v2.views.object_class.object_class_result import (
    MajorClass,
    ObjectClass,
    ObjectClassResults,
)
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


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


class ObjectClassSpendingViewSet(SpendingMixin, FabaOutlayMixin, PaginationMixin, DisasterBase):
    """View to implement the API"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/object_class/spending.md"

    @cache_response()
    def post(self, request):
        if self.spending_type == "award":
            self.has_children = True
            object_class_spending = self.get_covid_faba_spending(
                spending_level="object_class",
                def_codes=self.filters["def_codes"],
                columns_to_return=[
                    "funding_major_object_class_id",
                    "funding_major_object_class_code",
                    "funding_major_object_class_name",
                    "funding_object_class_id",
                    "funding_object_class_code",
                    "funding_object_class_name",
                ],
                award_types=self.filters.get("award_type_codes"),
                search_query=self.query,
                search_query_fields=["funding_major_object_class_name", "funding_object_class_name"],
            )
            json_result = self._build_json_result(object_class_spending)
            response = self.sort_json_result(
                data_to_sort=json_result,
                sort_key=self.pagination.sort_key,
                sort_order=self.pagination.sort_order,
                has_children=self.has_children,
            )

        else:
            results = list(self.total_queryset)
            extra_columns = []
            response = construct_response(results, self.pagination)
            response["totals"] = self.accumulate_total_values(results, extra_columns)

        return Response(response)

    @property
    def total_queryset(self):
        file_b_calculations = FileBCalculations(include_final_sub_filter=True, is_covid_page=True)
        filters = [
            self.is_in_provided_def_codes,
            self.all_closed_defc_submissions,
            file_b_calculations.is_non_zero_total_spending(),
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
            "obligation": Sum(file_b_calculations.get_obligations()),
            "outlay": Sum(file_b_calculations.get_outlays()),
            "award_count": Value(None, output_field=IntegerField()),
        }

        # Assuming it is more performant to fetch all rows once rather than
        #  run a count query and fetch only a page's worth of results
        return (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters)
            .values("object_class__major_object_class", "object_class__major_object_class_name")
            .annotate(**annotations)
            .values(*annotations.keys())
        )

    def _build_json_result(self, queryset: List[QuerySet]) -> dict:
        """Build the JSON response that will be returned for this endpoint.

        Args:
            queryset: Database query results.

        Returns:
            Formatted JSON response.
        """

        response = {"totals": {"award_count": 0, "obligation": 0, "outlay": 0}, "results": []}

        parent_lookup = {}
        child_lookup = {}

        for row in queryset:
            parent_major_object_class_id = row["funding_major_object_class_id"]

            if parent_major_object_class_id not in parent_lookup.keys():
                parent_lookup[parent_major_object_class_id] = {
                    "id": parent_major_object_class_id,
                    "code": row["funding_major_object_class_code"],
                    "description": row["funding_major_object_class_name"],
                    "award_count": 0,
                    "obligation": Decimal(0),
                    "outlay": Decimal(0),
                    "children": [],
                }

            if parent_major_object_class_id not in child_lookup.keys():
                child_lookup[parent_major_object_class_id] = [
                    {
                        "id": int(row["funding_object_class_id"]),
                        "code": row["funding_object_class_code"],
                        "description": row["funding_object_class_name"],
                        "award_count": int(row["award_count"]),
                        "obligation": Decimal(row["obligation_sum"]),
                        "outlay": Decimal(row["outlay_sum"]),
                    }
                ]
            else:
                child_lookup[parent_major_object_class_id].append(
                    {
                        "id": int(row["funding_object_class_id"]),
                        "code": row["funding_object_class_code"],
                        "description": row["funding_object_class_name"],
                        "award_count": int(row["award_count"]),
                        "obligation": Decimal(row["obligation_sum"]),
                        "outlay": Decimal(row["outlay_sum"]),
                    }
                )

            response["totals"]["obligation"] += Decimal(row["obligation_sum"])
            response["totals"]["outlay"] += Decimal(row["outlay_sum"])
            response["totals"]["award_count"] += row["award_count"]

        for parent_account_id, children in child_lookup.items():
            for child_ta_account in children:
                parent_lookup[parent_account_id]["children"].append(child_ta_account)
                parent_lookup[parent_account_id]["award_count"] += child_ta_account["award_count"]
                parent_lookup[parent_account_id]["obligation"] += child_ta_account["obligation"]
                parent_lookup[parent_account_id]["outlay"] += child_ta_account["outlay"]

        response["results"] = list(parent_lookup.values())

        response["page_metadata"] = get_pagination_metadata(
            len(response["results"]), self.pagination.limit, self.pagination.page
        )

        return response
