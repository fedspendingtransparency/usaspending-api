from django.db.models import Sum, Q, F
from django.db.models.functions import Coalesce
from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any, List
from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.references.models import GTASSF133Balances, BureauTitleLookup


class BureauFederalAccountList(PaginationMixin, AgencyBase):
    """
    Obtain the list of federal accounts and treasury accounts for a specific agency in a
    single fiscal year based on whether or not that federal account/treasury account has ever
    been submitted in File B.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/sub_components/bureau_slug.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year", "agency_type"]

    @property
    def bureau_slug(self):
        return self.kwargs["bureau_slug"]

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        self.sortable_columns = ["name", "id", "total_obligations", "total_outlays", "total_budgetary_resources"]
        self.default_sort_column = "total_obligations"
        results = sorted(
            self.get_federal_account_list(),
            key=lambda x: x.get(self.pagination.sort_key),
            reverse=self.pagination.sort_order == "desc",
        )
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        totals = self.get_totals(results)
        return Response(
            {
                "toptier_code": self.toptier_code,
                "bureau_slug": self.bureau_slug,
                "fiscal_year": self.fiscal_year,
                "totals": totals,
                "page_metadata": page_metadata,
                "results": results[self.pagination.lower_limit : self.pagination.upper_limit],
                "messages": self.standard_response_messages,
            }
        )

    def get_totals(self, results) -> dict:
        totals = {
            "total_obligations": sum(x["total_obligations"] for x in results),
            "total_outlays": sum(x["total_outlays"] for x in results),
            "total_budgetary_resources": sum(x["total_budgetary_resources"] for x in results),
        }
        return totals

    def get_federal_account_list(self) -> List[dict]:
        federal_accounts = [
            x["federal_account_code"]
            for x in BureauTitleLookup.objects.filter(bureau_slug=self.bureau_slug).values("federal_account_code")
        ]
        filters = [
            Q(
                treasury_account_identifier__federal_account__parent_toptier_agency_id=self.toptier_agency.toptier_agency_id
            ),
            Q(treasury_account_identifier__federal_account__federal_account_code__in=federal_accounts),
            Q(fiscal_year=self.fiscal_year),
            Q(fiscal_period=self.fiscal_period),
        ]
        last_period_results = (
            GTASSF133Balances.objects.filter(*filters)
            .filter(*filters)
            .annotate(
                name=F("treasury_account_identifier__federal_account__account_title"),
                account_code=F("treasury_account_identifier__federal_account__federal_account_code"),
            )
            .values("name", "account_code")
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
            .values("name", "account_code", "total_obligations", "total_outlays", "total_budgetary_resources")
        )
        results = [
            {
                "name": x["name"],
                "id": x["account_code"],
                "total_obligations": x["total_obligations"],
                "total_outlays": x["total_outlays"],
                "total_budgetary_resources": x["total_budgetary_resources"],
            }
            for x in last_period_results
        ]
        return results
