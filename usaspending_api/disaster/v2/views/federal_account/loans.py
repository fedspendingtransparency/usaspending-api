from decimal import Decimal
from typing import List

from django.db.models import F, Q, QuerySet, Sum
from rest_framework.response import Response

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.models import CovidFABASpending
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    FabaOutlayMixin,
    LoansMixin,
    LoansPaginationMixin,
)


class LoansViewSet(LoansMixin, LoansPaginationMixin, DisasterBase, FabaOutlayMixin):
    """Returns loan disaster spending by federal account."""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/federal_account/loans.md"

    @cache_response()
    def post(self, request):
        self.filters.update({"award_type_codes": ["07", "08"]})
        self.has_children = True

        account_db_results = self._get_account_covid_faba_spending()
        json_result = self._build_json_result(account_db_results)
        sorted_json_result = self._sort_json_result(json_result)

        return Response(sorted_json_result)

    def _get_account_covid_faba_spending(self) -> QuerySet:
        """Query the covid_faba_spending table and return COVID-19 FABA spending grouped by federal account.

        Returns:
            Database query results.
        """

        queryset = (
            CovidFABASpending.objects.filter(spending_level="treasury_account")
            .filter(award_type__in=self.filters["award_type_codes"])
            .filter(defc__in=self.filters["def_codes"])
            .values(
                "funding_federal_account_id",
                "funding_federal_account_code",
                "funding_federal_account_name",
                "funding_treasury_account_id",
                "funding_treasury_account_code",
                "funding_treasury_account_name",
            )
            .annotate(
                award_count=Sum("award_count"),
                obligation_sum=Sum("obligation_sum"),
                outlay_sum=Sum("outlay_sum"),
                face_value_of_loan=Sum("face_value_of_loan"),
            )
        )

        if self.query is not None:
            queryset = queryset.filter(
                Q(funding_federal_account_name__icontains=self.query)
                | Q(funding_treasury_account_name__icontains=self.query)
            )

        return queryset

    def _build_json_result(self, queryset: List[QuerySet]) -> dict:
        """Build the JSON response that will be returned for this endpoint.

        Args:
            queryset: Database query results.

        Returns:
            Formatted JSON response.
        """

        response = {"totals": {"obligation": 0, "outlay": 0, "award_count": 0, "face_value_of_loan": 0}, "results": []}

        parent_lookup = {}
        child_lookup = {}

        for row in queryset:
            parent_federal_account_id = int(row["funding_federal_account_id"])

            if parent_federal_account_id not in parent_lookup.keys():
                parent_lookup[parent_federal_account_id] = {
                    "id": parent_federal_account_id,
                    "code": row["funding_federal_account_code"],
                    "description": row["funding_federal_account_name"],
                    "award_count": 0,
                    "obligation": Decimal(0),
                    "outlay": Decimal(0),
                    "face_value_of_loan": Decimal(0),
                    "children": [],
                }

            if parent_federal_account_id not in child_lookup.keys():
                child_lookup[parent_federal_account_id] = [
                    {
                        "id": int(row["funding_treasury_account_id"]),
                        "code": row["funding_treasury_account_code"],
                        "description": row["funding_treasury_account_name"],
                        "award_count": int(row["award_count"]),
                        "obligation": Decimal(row["obligation_sum"]),
                        "outlay": Decimal(row["outlay_sum"]),
                        "face_value_of_loan": Decimal(row["face_value_of_loan"]),
                    }
                ]
            else:
                child_lookup[parent_federal_account_id].append(
                    {
                        "id": int(row["funding_treasury_account_id"]),
                        "code": row["funding_treasury_account_code"],
                        "description": row["funding_treasury_account_name"],
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

    def _sort_json_result(self, json_result: dict) -> dict:
        """Sort the JSON by the appropriate field and in the appropriate order before returning it.

        Args:
            json_result: Unsorted JSON result.

        Returns:
            Sorted JSON result.
        """

        if self.pagination.sort_key == "description":
            sorted_parents = sorted(
                json_result["results"],
                key=lambda val: val.get("description", "id").lower(),
                reverse=self.pagination.sort_order == "desc",
            )
        else:
            sorted_parents = sorted(
                json_result["results"],
                key=lambda val: val.get(self.pagination.sort_key, "id"),
                reverse=self.pagination.sort_order == "desc",
            )

        if self.has_children:
            for parent in sorted_parents:
                parent["children"] = sorted(
                    parent.get("children", []),
                    key=lambda val: val.get(self.pagination.sort_key, "id"),
                    reverse=self.pagination.sort_order == "desc",
                )

        json_result["results"] = sorted_parents

        return json_result

    @property
    def queryset(self):
        query = self.construct_loan_queryset(
            "treasury_account__treasury_account_identifier", TreasuryAppropriationAccount, "treasury_account_identifier"
        )

        annotations = {
            "fa_code": F("federal_account__federal_account_code"),
            "award_count": query.award_count_column,
            "description": F("account_title"),
            "code": F("tas_rendering_label"),
            "id": F("treasury_account_identifier"),
            "fa_description": F("federal_account__account_title"),
            "fa_id": F("federal_account_id"),
            "obligation": query.obligation_column,
            "outlay": query.outlay_column,
            # hack to use the Dataclasses, will be renamed later
            "total_budgetary_resources": query.face_value_of_loan_column,
        }

        return query.queryset.annotate(**annotations).values(*annotations)
