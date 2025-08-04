from rest_framework.response import Response
from django.db.models import F
from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.reporting.models import ReportingAgencyMissingTas


class AgencyDiscrepancies(PaginationMixin, AgencyBase):
    """Returns TAS discrepancies of the specified agency's submission data for a specific FY/FP"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/toptier_code/discrepancies.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year", "fiscal_period"]

    def get(self, request, toptier_code):
        self.sortable_columns = [
            "amount",
            "tas",
        ]
        self.default_sort_column = "amount"
        results = self.get_agency_discrepancies(fiscal_year=self.fiscal_year, fiscal_period=self.fiscal_period)
        return Response(
            {
                "page_metadata": get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page),
                "results": results[self.pagination.lower_limit : self.pagination.upper_limit],
                "messages": self.standard_response_messages,
            }
        )

    def get_agency_discrepancies(self, fiscal_year, fiscal_period):
        result_list = (
            ReportingAgencyMissingTas.objects.filter(
                toptier_code=self.toptier_code, fiscal_year=fiscal_year, fiscal_period=fiscal_period
            )
            .exclude(obligated_amount=0)
            .annotate(tas=F("tas_rendering_label"), amount=F("obligated_amount"))
            .values("tas", "amount")
        )
        return sorted(
            result_list,
            key=lambda x: (x["amount"], x["tas"]) if self.pagination.sort_key == "amount" else x["tas"],
            reverse=self.pagination.sort_order == "desc",
        )
