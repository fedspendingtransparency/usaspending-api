from django.db.models import Sum
from rest_framework.response import Response
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.fiscal_year_helpers import (
    calculate_last_completed_fiscal_quarter,
    convert_fiscal_quarter_to_fiscal_period,
)


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
            submission__reporting_fiscal_year=self.fiscal_year, submission__reporting_fiscal_period=self.fiscal_period
        ).aggregate(total_federal_budgetary_resources=Sum("total_budgetary_resources_amount_cpe"))[
            "total_federal_budgetary_resources"
        ]

    def get_agency_budgetary_resources(self):
        aab = AppropriationAccountBalances.objects.filter(
            submission__reporting_fiscal_year=self.fiscal_year,
            submission__reporting_fiscal_period=self.fiscal_period,
            treasury_account_identifier__funding_toptier_agency=self.toptier_agency,
        ).aggregate(
            agency_budgetary_resources=Sum("total_budgetary_resources_amount_cpe"),
            agency_total_obligated=Sum("obligations_incurred_total_by_tas_cpe"),
        )
        return aab["agency_budgetary_resources"], aab["agency_total_obligated"]

    def get_prior_year_agency_budgetary_resources(self):
        """
        Even though we're looking at fiscal_year - 1, it's possible that fiscal year hasn't been closed out
        yet.  For example, if today is 5 Oct 1999 then the submission window for FY 1999 is not closed yet
        even though we're in FY2000 Q1.
        """
        prior_fiscal_year = self.fiscal_year - 1
        prior_fiscal_year_last_completed_fiscal_period = convert_fiscal_quarter_to_fiscal_period(
            calculate_last_completed_fiscal_quarter(prior_fiscal_year)
        )
        return AppropriationAccountBalances.objects.filter(
            submission__reporting_fiscal_year=prior_fiscal_year,
            submission__reporting_fiscal_period=prior_fiscal_year_last_completed_fiscal_period,
            treasury_account_identifier__funding_toptier_agency=self.toptier_agency,
        ).aggregate(agency_budgetary_resources=Sum("total_budgetary_resources_amount_cpe"))[
            "agency_budgetary_resources"
        ]

    def get_agency_obligations_by_period(self):
        """
        We limit periods to 3, 6, 9, and 12 (quarters) but with the CARES Act coming down the road
        we will need to figure out how to include monthly submissions.  I am kicking that bucket down
        the road until the CARE Act details have been more fully fleshed out.
        """
        fiscal_periods = [n for n in (3, 6, 9, 12) if n <= self.fiscal_period]
        return [
            {
                "period": abb["submission__reporting_fiscal_period"],
                "obligated": abb["obligations_incurred_total_by_tas_cpe__sum"],
            }
            for abb in (
                AppropriationAccountBalances.objects.filter(
                    submission__reporting_fiscal_year=self.fiscal_year,
                    submission__reporting_fiscal_period__in=fiscal_periods,
                    treasury_account_identifier__funding_toptier_agency=self.toptier_agency,
                )
                .values("submission__reporting_fiscal_period")
                .annotate(Sum("obligations_incurred_total_by_tas_cpe"))
                .order_by("submission__reporting_fiscal_period")
            )
        ]
