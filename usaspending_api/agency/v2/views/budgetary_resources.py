from django.db.models import Sum
from rest_framework.response import Response
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.date_helper import now
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year
from usaspending_api.references.models import GTASSF133Balances


class BudgetaryResources(AgencyBase):
    """
    Returns budgetary resources and obligations for the agency and fiscal year requested.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/budgetary_resources.md"

    @cache_response()
    def get(self, request, *args, **kwargs):
        return Response(
            {
                "toptier_code": self.toptier_agency.toptier_code,
                "agency_data_by_year": self.get_agency_budgetary_resources(),
                "messages": self.standard_response_messages,
            }
        )

    def get_total_federal_budgetary_resources(self):
        return (
            GTASSF133Balances.objects.values("fiscal_year")
            .annotate(total_budgetary_resources=Sum("total_budgetary_resources_cpe"))
            .values("fiscal_year", "total_budgetary_resources")
        )

    def get_agency_budgetary_resources(self):
        aab = (
            AppropriationAccountBalances.objects.filter(
                treasury_account_identifier__funding_toptier_agency=self.toptier_agency,
                submission__submission_window__submission_reveal_date__lte=now(),
            )
            .values("submission__reporting_fiscal_year")
            .annotate(
                agency_budgetary_resources=Sum("total_budgetary_resources_amount_cpe"),
                agency_total_obligated=Sum("obligations_incurred_total_by_tas_cpe"),
            )
        )
        fbr = self.get_total_federal_budgetary_resources()
        resources = {}
        for z in fbr:
            resources.update({z["fiscal_year"]: z["total_budgetary_resources"]})
        results = [
            {
                "fiscal_year": x["submission__reporting_fiscal_year"],
                "agency_budgetary_resources": x["agency_budgetary_resources"],
                "agency_total_obligated": x["agency_total_obligated"],
                "federal_budgetary_resources": resources.get(x["submission__reporting_fiscal_year"]),
            }
            for x in aab
        ]
        years = [x["fiscal_year"] for x in results]
        for year in range(2017, current_fiscal_year() + 1):
            if year not in years:
                results.append(
                    {
                        "fiscal_year": year,
                        "agency_budgetary_resources": None,
                        "agency_total_obligated": None,
                        "federal_budgetary_resources": resources.get(year),
                    }
                )
        return sorted(results, key=lambda x: x["fiscal_year"], reverse=True)
