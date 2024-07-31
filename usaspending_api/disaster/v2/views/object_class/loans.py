from decimal import Decimal
from typing import List

from django.db.models import F, Min, QuerySet, TextField, Value
from django.db.models.functions import Cast
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.helpers.orm_helpers import ConcatAll
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    FabaOutlayMixin,
    LoansMixin,
    LoansPaginationMixin,
)
from usaspending_api.references.models import ObjectClass


class ObjectClassLoansViewSet(LoansMixin, FabaOutlayMixin, LoansPaginationMixin, DisasterBase):
    """Provides insights on the Object Classes' loans from disaster/emergency funding per the requested filters"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/object_class/loans.md"

    @cache_response()
    def post(self, request):
        self.filters.update({"award_type_codes": ["07", "08"]})
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
            award_types=self.filters["award_type_codes"],
            search_query=self.query,
            search_query_fields=["funding_major_object_class_name", "funding_object_class_name"],
        )
        json_result = self._build_json_result(object_class_spending)
        sorted_json_result = self.sort_json_result(
            data_to_sort=json_result,
            sort_key=self.pagination.sort_key,
            sort_order=self.pagination.sort_order,
            has_children=self.has_children,
        )

        return Response(sorted_json_result)

    def _build_json_result(self, queryset: List[QuerySet]) -> dict:
        """Build the JSON response that will be returned for this endpoint.

        Args:
            queryset: Database query results.

        Returns:
            Formatted JSON response.
        """

        response = {"totals": {"award_count": 0, "face_value_of_loan": 0, "obligation": 0, "outlay": 0}, "results": []}

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
                    "face_value_of_loan": Decimal(0),
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
                        "face_value_of_loan": Decimal(row["face_value_of_loan"]),
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
                        "face_value_of_loan": Decimal(row["face_value_of_loan"]),
                    }
                )

            response["totals"]["obligation"] += Decimal(row["obligation_sum"])
            response["totals"]["outlay"] += Decimal(row["outlay_sum"])
            response["totals"]["award_count"] += row["award_count"]
            response["totals"]["face_value_of_loan"] += Decimal(row["face_value_of_loan"])

        for parent_account_id, children in child_lookup.items():
            for child_ta_account in children:
                parent_lookup[parent_account_id]["children"].append(child_ta_account)
                parent_lookup[parent_account_id]["award_count"] += child_ta_account["award_count"]
                parent_lookup[parent_account_id]["obligation"] += child_ta_account["obligation"]
                parent_lookup[parent_account_id]["outlay"] += child_ta_account["outlay"]
                parent_lookup[parent_account_id]["face_value_of_loan"] += child_ta_account["face_value_of_loan"]

        response["results"] = list(parent_lookup.values())

        response["page_metadata"] = get_pagination_metadata(
            len(response["results"]), self.pagination.limit, self.pagination.page
        )

        return response

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
