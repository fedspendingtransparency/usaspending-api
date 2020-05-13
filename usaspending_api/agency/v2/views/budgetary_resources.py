from django.db.models import Sum
from rest_framework.response import Response
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.cache_decorator import cache_response


class BudgetaryResources(AgencyBase):
    """
    Returns budgetary resources and obligations for the agency and fiscal year requested.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/budgetary_resources.md"

    @cache_response()
    def get(self, request, *args, **kwargs):
        agency_budgetary_resources, agency_total_obligated = self.get_agency_budgetary_resources()
        return Response(
            {
                "fiscal_year": self.fiscal_year,
                "toptier_code": self.toptier_agency.toptier_code,
                "agency_budgetary_resources": agency_budgetary_resources,
                "prior_year_agency_budgetary_resources": self.get_prior_year_agency_budgetary_resources(),
                "total_federal_budgetary_resources": self.get_total_federal_budgetary_resources(),
                "agency_total_obligated": agency_total_obligated,
                "agency_obligation_by_period": self.get_agency_obligations_by_period(),
                "messages": self.standard_response_messages,
            }
        )

    def get_total_federal_budgetary_resources(self):
        """
        IMPORTANT NOTE!  This is placeholder functionality.  DEV-4014 addresses the underlying problem of
        not having actual historical budgetary resources.  This code is here to provide semi-reasonable
        values for development and testing until such time as legitimate values are available.  A note
        has been added to DEV-4014 to rectify this once that ticket has been resolved.
        """
        return AppropriationAccountBalances.objects.filter(
            submission__reporting_fiscal_year=self.fiscal_year, final_of_fy=True
        ).aggregate(total_federal_budgetary_resources=Sum("total_budgetary_resources_amount_cpe"))[
            "total_federal_budgetary_resources"
        ]

    def get_agency_budgetary_resources(self):
        aab = AppropriationAccountBalances.objects.filter(
            submission__reporting_fiscal_year=self.fiscal_year,
            treasury_account_identifier__funding_toptier_agency=self.toptier_agency,
            final_of_fy=True,
        ).aggregate(
            agency_budgetary_resources=Sum("total_budgetary_resources_amount_cpe"),
            agency_total_obligated=Sum("obligations_incurred_total_by_tas_cpe"),
        )
        return aab["agency_budgetary_resources"], aab["agency_total_obligated"]

    def get_prior_year_agency_budgetary_resources(self):
        return AppropriationAccountBalances.objects.filter(
            submission__reporting_fiscal_year=self.fiscal_year - 1,
            treasury_account_identifier__funding_toptier_agency=self.toptier_agency,
            final_of_fy=True,
        ).aggregate(agency_budgetary_resources=Sum("total_budgetary_resources_amount_cpe"))[
            "agency_budgetary_resources"
        ]

    def get_agency_obligations_by_period(self):
        return [
            {
                "period": abb["submission__reporting_fiscal_period"],
                "obligated": abb["obligations_incurred_total_by_tas_cpe__sum"],
            }
            for abb in (
                AppropriationAccountBalances.objects.filter(
                    submission__reporting_fiscal_year=self.fiscal_year,
                    treasury_account_identifier__funding_toptier_agency=self.toptier_agency,
                )
                .values("submission__reporting_fiscal_period")
                .annotate(Sum("obligations_incurred_total_by_tas_cpe"))
                .order_by("submission__reporting_fiscal_period")
            )
        ]
