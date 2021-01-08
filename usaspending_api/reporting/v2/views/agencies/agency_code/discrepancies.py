from rest_framework.response import Response
from django.db.models import F
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.reporting.models import ReportingAgencyMissingTas


class AgencyDiscrepancies(AgencyBase, PaginationMixin):
    """Returns TAS discrepancies of the specified agency's submission data for a specific FY/FP"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/agency_code/discrepancies.md"

    def get(self, request, toptier_code):
        model = [
            {
                "key": "fiscal_year",
                "name": "fiscal_year",
                "type": "integer",
                "min": 2017,
                "optional": False,
                "default": None,
                "allow_nulls": False,
            },
            {
                "key": "fiscal_period",
                "name": "fiscal_period",
                "type": "integer",
                "min": 2,
                "max": 12,
                "optional": False,
                "default": None,
                "allow_nulls": False,
            },
        ]
        validated = TinyShield(model).block(request.query_params)
        self.sortable_columns = [
            "amount",
            "tas",
        ]
        self.default_sort_column = "amount"
        results = self.get_agency_discrepancies(
            fiscal_year=validated["fiscal_year"], fiscal_period=validated["fiscal_period"]
        )
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
            .values("tas_rendering_label", "obligated_amount")
            .annotate(tas=F("tas_rendering_label"), amount=F("obligated_amount"))
        )
        results = [{"tas": result["tas"], "amount": result["amount"]} for result in result_list]
        return sorted(
            results,
            key=lambda x: (x["amount"], x["tas"]) if self.pagination.sort_key == "amount" else x["tas"],
            reverse=self.pagination.sort_order == "desc",
        )
