from django.db.models import Sum
from rest_framework.response import Response
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.date_helper import now


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

    def get_total_federal_budgetary_resources(self, fiscal_year):
        """
        IMPORTANT NOTE!  This is placeholder functionality.  DEV-4014 addresses the underlying problem of
        not having actual historical budgetary resources.  This code is here to provide semi-reasonable
        values for development and testing until such time as legitimate values are available.  A note
        has been added to DEV-4014 to rectify this once that ticket has been resolved.
        """
        return AppropriationAccountBalances.objects.filter(submission__reporting_fiscal_year=fiscal_year).aggregate(
            total_federal_budgetary_resources=Sum("total_budgetary_resources_amount_cpe")
        )["total_federal_budgetary_resources"]

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
        return sorted(
            [
                {
                    "fiscal_year": x["submission__reporting_fiscal_year"],
                    "agency_budgetary_resources": x["agency_budgetary_resources"],
                    "agency_total_obligated": x["agency_total_obligated"],
                    "federal_budgetary_resources": self.get_total_federal_budgetary_resources(
                        x["submission__reporting_fiscal_year"]
                    ),
                }
                for x in aab
            ],
            key=lambda x: x["fiscal_year"],
            reverse=True,
        )
