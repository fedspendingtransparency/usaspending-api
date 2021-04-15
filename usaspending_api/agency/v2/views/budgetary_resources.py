from django.db.models import Sum, Max, F, Q
from rest_framework.response import Response
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.date_helper import now
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year
from usaspending_api.references.models import GTASSF133Balances
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule


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
        submission_windows = (
            DABSSubmissionWindowSchedule.objects.filter(submission_reveal_date__lte=now())
            .values("submission_fiscal_year")
            .annotate(fiscal_year=F("submission_fiscal_year"), fiscal_period=Max("submission_fiscal_month"))
        )
        q = Q()
        for sub in submission_windows:
            q |= Q(fiscal_year=sub["fiscal_year"]) & Q(fiscal_period=sub["fiscal_period"])
        results = (
            GTASSF133Balances.objects.filter(q)
            .values("fiscal_year")
            .annotate(total_budgetary_resources=Sum("total_budgetary_resources_cpe"))
            .values("fiscal_year", "total_budgetary_resources")
        )
        return results

    def get_periods_by_year(self, fiscal_year):
        return [
            {
                "period": abb["submission__reporting_fiscal_period"],
                "obligated": abb["obligations_incurred_total_by_tas_cpe__sum"],
            }
            for abb in (
                AppropriationAccountBalances.objects.filter(
                    submission__reporting_fiscal_year=fiscal_year,
                    treasury_account_identifier__funding_toptier_agency=self.toptier_agency,
                    submission__submission_window__submission_reveal_date__lte=now(),
                )
                .values("submission__reporting_fiscal_period")
                .annotate(Sum("obligations_incurred_total_by_tas_cpe"))
                .order_by("submission__reporting_fiscal_period")
            )
        ]

    def get_agency_budgetary_resources(self):
        aab = (
            AppropriationAccountBalances.objects.filter(
                treasury_account_identifier__funding_toptier_agency=self.toptier_agency,
                submission__submission_window__submission_reveal_date__lte=now(),
                submission__is_final_balances_for_fy=True,
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
                "total_budgetary_resources": resources.get(x["submission__reporting_fiscal_year"]),
                "agency_obligation_by_period": self.get_periods_by_year(x["submission__reporting_fiscal_year"]),
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
                        "total_budgetary_resources": resources.get(year),
                        "agency_obligation_by_period": [],
                    }
                )
        return sorted(results, key=lambda x: x["fiscal_year"], reverse=True)
