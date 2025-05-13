from django.db.models import Sum, Max, F, Q
from rest_framework.response import Response
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.calculations.file_b import FileBCalculations
from usaspending_api.common.helpers.date_helper import now
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import GTASSF133Balances
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule


class BudgetaryResources(AgencyBase):
    """
    Returns budgetary resources and obligations for the agency and fiscal year requested.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/budgetary_resources.md"

    file_b_calulcations = FileBCalculations()

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

    def get_periods_by_year(self):
        periods = {}
        fabpaoc = (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(
                treasury_account__funding_toptier_agency=self.toptier_agency,
                submission__submission_window__submission_reveal_date__lte=now(),
            )
            .values("submission__reporting_fiscal_year", "submission__reporting_fiscal_period")
            .annotate(
                fiscal_year=F("submission__reporting_fiscal_year"),
                fiscal_period=F("submission__reporting_fiscal_period"),
                obligation_sum=Sum(self.file_b_calulcations.get_obligations()),
            )
            .order_by("fiscal_year", "fiscal_period")
        )

        for val in fabpaoc:
            # This "continue" logic is in place to prevent the case where multiple agencies have submitted a mixture of
            # quarterly and monthly with a single agency listed as the funding agency. In this case the total
            # obligations are not displayed correctly on the monthly submissions that do not line up with a
            # corresponding quarterly submission.
            # TODO: Update with logic that takes into account the time period before required monthly submissions
            if val["fiscal_year"] < 2022 and val["fiscal_period"] not in (3, 6, 9, 12):
                continue
            if periods.get(val["fiscal_year"]) is not None:
                periods[val["fiscal_year"]].append(
                    {
                        "period": val["fiscal_period"],
                        "obligated": val["obligation_sum"],
                    }
                )
            else:
                periods.update(
                    {
                        val["fiscal_year"]: [
                            {
                                "period": val["fiscal_period"],
                                "obligated": val["obligation_sum"],
                            }
                        ]
                    }
                )
        return periods

    def get_agency_budgetary_resources(self):
        aab = (
            AppropriationAccountBalances.objects.filter(
                treasury_account_identifier__funding_toptier_agency=self.toptier_agency,
                submission__submission_window__submission_reveal_date__lte=now(),
                submission__is_final_balances_for_fy=True,
            )
            .values("submission__reporting_fiscal_year")
            .annotate(agency_budgetary_resources=Sum("total_budgetary_resources_amount_cpe"))
        )
        aab_by_year = {val["submission__reporting_fiscal_year"]: val for val in aab}

        fabpaoc = (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(
                treasury_account__funding_toptier_agency=self.toptier_agency,
                submission__submission_window__submission_reveal_date__lte=now(),
                submission__is_final_balances_for_fy=True,
            )
            .values("submission__reporting_fiscal_year")
            .annotate(
                agency_total_obligated=Sum(self.file_b_calulcations.get_obligations()),
                agency_total_outlayed=Sum(self.file_b_calulcations.get_outlays()),
            )
        )
        fabpaoc_by_year = {val["submission__reporting_fiscal_year"]: val for val in fabpaoc}

        fbr = self.get_total_federal_budgetary_resources()
        resources = {val["fiscal_year"]: val["total_budgetary_resources"] for val in fbr}
        periods_by_year = self.get_periods_by_year()

        results = []
        for year in range(2017, current_fiscal_year() + 1):
            if year not in aab_by_year:
                results.append(
                    {
                        "fiscal_year": year,
                        "agency_budgetary_resources": None,
                        "agency_total_obligated": None,
                        "agency_total_outlayed": None,
                        "total_budgetary_resources": resources.get(year),
                        "agency_obligation_by_period": [],
                    }
                )
            else:
                results.append(
                    {
                        "fiscal_year": year,
                        "agency_budgetary_resources": aab_by_year[year]["agency_budgetary_resources"],
                        "agency_total_obligated": fabpaoc_by_year.get(year, {}).get("agency_total_obligated"),
                        "agency_total_outlayed": fabpaoc_by_year.get(year, {}).get("agency_total_outlayed"),
                        "total_budgetary_resources": resources.get(year),
                        "agency_obligation_by_period": periods_by_year.get(year, []),
                    }
                )

        return sorted(results, key=lambda x: x["fiscal_year"], reverse=True)
