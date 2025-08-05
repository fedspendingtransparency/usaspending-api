from decimal import Decimal
from typing import List

from django.db.models import (
    DecimalField,
    F,
    Func,
    IntegerField,
    OuterRef,
    Q,
    QuerySet,
    Subquery,
    Sum,
    Value,
)
from django.db.models.functions import Coalesce
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
    latest_gtas_of_each_year_queryset,
)
from usaspending_api.disaster.v2.views.federal_account.federal_account_result import (
    TAS,
    FedAccount,
    FedAcctResults,
)
from usaspending_api.financial_activities.models import (
    FinancialAccountsByProgramActivityObjectClass,
)


def construct_response(results: list, pagination: Pagination):
    FederalAccounts = FedAcctResults()
    for row in results:
        FA = FedAccount(
            id=row.pop("fa_id"),
            code=row.pop("fa_code"),
            award_count=0,
            description=row.pop("fa_description"),
        )
        FederalAccounts[FA].include(TAS(**row))

    return {
        "results": FederalAccounts.finalize(pagination),
        "page_metadata": get_pagination_metadata(len(FederalAccounts), pagination.limit, pagination.page),
    }


class SpendingViewSet(SpendingMixin, FabaOutlayMixin, PaginationMixin, DisasterBase):
    """Returns disaster spending by federal account."""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/federal_account/spending.md"

    @cache_response()
    def post(self, request):
        if self.spending_type == "award":
            self.has_children = True
            account_db_results = self.get_covid_faba_spending(
                spending_level="treasury_account",
                def_codes=self.filters["def_codes"],
                columns_to_return=[
                    "funding_federal_account_id",
                    "funding_federal_account_code",
                    "funding_federal_account_name",
                    "funding_treasury_account_id",
                    "funding_treasury_account_code",
                    "funding_treasury_account_name",
                ],
                search_query=self.query,
                search_query_fields=["funding_federal_account_name", "funding_treasury_account_name"],
            )
            json_result = self._build_json_result(account_db_results)
            sorted_json_result = self.sort_json_result(
                json_result, self.pagination.sort_key, self.pagination.sort_order, has_children=self.has_children
            )
            return Response(sorted_json_result)
        else:
            results = list(self.total_queryset)
            extra_columns = ["total_budgetary_resources"]
        response = construct_response(results, self.pagination)
        response["totals"] = self.accumulate_total_values(results, extra_columns)

        return Response(response)

    def _build_json_result(self, queryset: List[QuerySet]) -> dict:
        """Build the JSON response that will be returned for this endpoint.

        Args:
            queryset: Database query results.

        Returns:
            Formatted JSON response.
        """

        results = {}
        response = {
            "totals": {"obligation": 0, "outlay": 0, "award_count": 0},
            "results": [],
        }
        for row in queryset:
            parent_fa_id = int(row["funding_federal_account_id"])
            if results.get(parent_fa_id) is None:
                results[parent_fa_id] = {
                    "id": parent_fa_id,
                    "code": row["funding_federal_account_code"],
                    "description": row["funding_federal_account_name"],
                    "award_count": 0,
                    "obligation": Decimal(0),
                    "outlay": Decimal(0),
                    "total_budgetary_resources": None,  # This type of response never has a TBR value
                    "children": [],
                }
            results[parent_fa_id]["children"].append(
                {
                    "id": int(row["funding_treasury_account_id"]),
                    "code": row["funding_treasury_account_code"],
                    "description": row["funding_treasury_account_name"],
                    "award_count": int(row["award_count"]),
                    "obligation": Decimal(row["obligation_sum"]),
                    "outlay": Decimal(row["outlay_sum"]),
                    "total_budgetary_resources": None,  # This spending_type doesn't have TBR
                }
            )
            results[parent_fa_id]["obligation"] += Decimal(row["obligation_sum"])
            results[parent_fa_id]["outlay"] += Decimal(row["outlay_sum"])
            results[parent_fa_id]["award_count"] += row["award_count"]
            response["totals"]["obligation"] += Decimal(row["obligation_sum"])
            response["totals"]["outlay"] += Decimal(row["outlay_sum"])
            response["totals"]["award_count"] += row["award_count"]
        response["results"] = list(results.values())
        response["page_metadata"] = get_pagination_metadata(
            len(response["results"]), self.pagination.limit, self.pagination.page
        )
        return response

    @property
    def total_queryset(self):
        file_b_calculations = FileBCalculations(include_final_sub_filter=True, is_covid_page=True)
        filters = [
            self.is_in_provided_def_codes,
            file_b_calculations.is_non_zero_total_spending(),
            self.all_closed_defc_submissions,
            Q(treasury_account__isnull=False),
            Q(treasury_account__federal_account__isnull=False),
        ]

        annotations = {
            "fa_code": F("treasury_account__federal_account__federal_account_code"),
            "description": F("treasury_account__account_title"),
            "code": F("treasury_account__tas_rendering_label"),
            "id": F("treasury_account__treasury_account_identifier"),
            "award_count": Value(None, output_field=IntegerField()),
            "fa_description": F("treasury_account__federal_account__account_title"),
            "fa_id": F("treasury_account__federal_account_id"),
            "obligation": Sum(file_b_calculations.get_obligations()),
            "outlay": Sum(file_b_calculations.get_outlays()),
            "total_budgetary_resources": Coalesce(
                Subquery(
                    latest_gtas_of_each_year_queryset()
                    .filter(
                        disaster_emergency_fund_id__in=self.def_codes,
                        treasury_account_identifier=OuterRef("treasury_account"),
                    )
                    .annotate(
                        amount=Func("total_budgetary_resources_cpe", function="Sum"),
                        unobligated_balance=Func(
                            "budget_authority_unobligated_balance_brought_forward_cpe",
                            function="Sum",
                        ),
                        deobligation=Func(
                            "deobligations_or_recoveries_or_refunds_from_prior_year_cpe",
                            function="Sum",
                        ),
                        prior_year=Func("prior_year_paid_obligation_recoveries", function="Sum"),
                        unobligated_adjustments=Func(
                            "adjustments_to_unobligated_balance_brought_forward_fyb",
                            function="Sum",
                        ),
                    )
                    .annotate(
                        total_budget_authority=F("amount")
                        - F("unobligated_balance")
                        - F("deobligation")
                        - F("prior_year")
                        - F("unobligated_adjustments")
                    )
                    .values("total_budget_authority"),
                    output_field=DecimalField(max_digits=23, decimal_places=2),
                ),
                0,
                output_field=DecimalField(max_digits=23, decimal_places=2),
            ),
        }

        # Assuming it is more performant to fetch all rows once rather than
        #  run a count query and fetch only a page's worth of results
        return (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters)
            .values(
                "treasury_account__federal_account__id",
                "treasury_account__federal_account__federal_account_code",
                "treasury_account__federal_account__account_title",
            )
            .annotate(**annotations)
            .values(*annotations.keys())
        )
