from django.db.models import Q, Sum, OuterRef, Subquery, F, Value
from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any
from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.helpers.orm_helpers import ConcatAll
from usaspending_api.references.models import GTASSF133Balances, BureauTitleLookup


class SubcomponentList(PaginationMixin, AgencyBase):
    """
    Obtain the count of subcomponents (bureaus) for a specific agency in a single
    fiscal year based on GTAS
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/sub_components.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["toptier_code", "fiscal_year"]

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        self.sortable_columns = ["name", "total_obligations", "total_outlays", "total_budgetary_resources"]
        self.default_sort_column = "total_obligations"
        results = self.format_results(self.get_subcomponents_queryset())
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        return Response(
            {
                "toptier_code": self.toptier_code,
                "fiscal_year": self.fiscal_year,
                "results": results[self.pagination.lower_limit : self.pagination.upper_limit],
                "messages": self.standard_response_messages,
                "page_metadata": page_metadata,
            }
        )

    def format_results(self, response):
        results = sorted(
            [
                {
                    "name": x["bureau_info"].split(";")[0] if x.get("bureau_info") is not None else None,
                    "id": x["bureau_info"].split(";")[1] if x.get("bureau_info") is not None else None,
                    "total_obligations": x["total_obligations"],
                    "total_outlays": x["total_outlays"],
                    "total_budgetary_resources": x["total_budgetary_resources"],
                }
                for x in response
            ],
            key=lambda x: x.get(self.pagination.sort_key),
            reverse=self.pagination.sort_order == "desc",
        )
        return results

    def get_subcomponents_queryset(self):
        filters = [
            Q(treasury_account_identifier__federal_account__parent_toptier_agency=self.toptier_agency),
            Q(fiscal_year=self.fiscal_year),
            Q(fiscal_period=self.fiscal_period),
        ]

        results = (
            (GTASSF133Balances.objects.filter(*filters))
            .annotate(
                bureau_info=Subquery(
                    BureauTitleLookup.objects.filter(
                        federal_account_code=OuterRef(
                            "treasury_account_identifier__federal_account__federal_account_code"
                        )
                    )
                    .annotate(bureau_info=ConcatAll(F("bureau_title"), Value(";"), F("bureau_slug")))
                    .values("bureau_info")
                )
            )
            .values("bureau_info")
            .annotate(
                amount=Sum("total_budgetary_resources_cpe"),
                unobligated_balance=Sum("budget_authority_unobligated_balance_brought_forward_cpe"),
                deobligation=Sum("deobligations_or_recoveries_or_refunds_from_prior_year_cpe"),
                prior_year=Sum("prior_year_paid_obligation_recoveries"),
            )
            .annotate(
                total_budgetary_resources=F("amount") - F("unobligated_balance") - F("deobligation") - F("prior_year"),
                total_obligations=Sum("obligations_incurred_total_cpe")
                - Sum("deobligations_or_recoveries_or_refunds_from_prior_year_cpe"),
                total_outlays=Sum("gross_outlay_amount_by_tas_cpe")
                - Sum("anticipated_prior_year_obligation_recoveries"),
            )
            .values("bureau_info", "total_obligations", "total_outlays", "total_budgetary_resources")
        )
        return results
