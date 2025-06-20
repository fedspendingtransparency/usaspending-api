from django.db.models import DecimalField, F, Func, IntegerField, OuterRef, Q, Subquery
from rest_framework.response import Response

from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.references.models import GTASSF133Balances, ToptierAgency
from usaspending_api.reporting.models import ReportingAgencyMissingTas, ReportingAgencyOverview, ReportingAgencyTas
from usaspending_api.submissions.helpers import is_valid_monthly_period
from usaspending_api.submissions.models import SubmissionAttributes


class AgencyOverview(PaginationMixin, AgencyBase):
    """Returns an overview of the specified agency's submission data"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/toptier_code/overview.md"

    @cache_response()
    def get(self, request, toptier_code):
        self.sortable_columns = [
            "current_total_budget_authority_amount",
            "fiscal_year",
            "missing_tas_accounts_count",
            "tas_accounts_total",
            "obligation_difference",
            "percent_of_total_budgetary_resources",
            "recent_publication_date",
            "recent_publication_date_certified",
            "tas_obligation_not_in_gtas_total",
            "unlinked_contract_award_count",
            "unlinked_assistance_award_count",
        ]

        self.default_sort_column = "current_total_budget_authority_amount"
        results = self.get_agency_overview()
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        results = results[self.pagination.lower_limit : self.pagination.upper_limit]
        return Response(
            {"page_metadata": page_metadata, "results": results, "messages": self.standard_response_messages}
        )

    def get_agency_overview(self):

        toptier_code_filter = Q(toptier_code=OuterRef("toptier_code"))
        agency_filters = [
            Q(reporting_fiscal_year=OuterRef("fiscal_year")),
            Q(reporting_fiscal_period=OuterRef("fiscal_period")),
            Q(toptier_code=OuterRef("toptier_code")),
        ]
        result_list = (
            ReportingAgencyOverview.objects.filter(toptier_code=self.toptier_code)
            .annotate(
                agency_name=Subquery(ToptierAgency.objects.filter(toptier_code_filter).values("name")),
                abbreviation=Subquery(ToptierAgency.objects.filter(toptier_code_filter).values("abbreviation")),
                recent_publication_date=Subquery(
                    SubmissionAttributes.objects.filter(*agency_filters).values("published_date")
                ),
                recent_publication_date_certified=Subquery(
                    SubmissionAttributes.objects.filter(*agency_filters).values("certified_date")
                ),
                submission_is_quarter=Subquery(
                    SubmissionAttributes.objects.filter(*agency_filters).values("quarter_format_flag")
                ),
                tas_obligations=Subquery(
                    ReportingAgencyTas.objects.filter(
                        fiscal_year=OuterRef("fiscal_year"),
                        fiscal_period=OuterRef("fiscal_period"),
                        toptier_code=OuterRef("toptier_code"),
                    )
                    .annotate(the_sum=Func(F("appropriation_obligated_amount"), function="SUM"))
                    .values("the_sum"),
                    output_field=DecimalField(max_digits=23, decimal_places=2),
                ),
                tas_obligation_not_in_gtas_total=Subquery(
                    ReportingAgencyMissingTas.objects.filter(
                        fiscal_year=OuterRef("fiscal_year"),
                        fiscal_period=OuterRef("fiscal_period"),
                        toptier_code=OuterRef("toptier_code"),
                    )
                    .annotate(the_sum=Func(F("obligated_amount"), function="SUM"))
                    .values("the_sum"),
                    output_field=DecimalField(max_digits=23, decimal_places=2),
                ),
                missing_tas_accounts=Subquery(
                    ReportingAgencyMissingTas.objects.filter(
                        fiscal_year=OuterRef("fiscal_year"),
                        fiscal_period=OuterRef("fiscal_period"),
                        toptier_code=OuterRef("toptier_code"),
                    )
                    .exclude(obligated_amount=0)
                    .annotate(count=Func(F("tas_rendering_label"), function="COUNT"))
                    .values("count"),
                    output_field=IntegerField(),
                ),
                gtas_total_budgetary_resources=Subquery(
                    GTASSF133Balances.objects.filter(
                        fiscal_year=OuterRef("fiscal_year"), fiscal_period=OuterRef("fiscal_period")
                    )
                    .annotate(the_sum=Func(F("total_budgetary_resources_cpe"), function="SUM"))
                    .values("the_sum"),
                    output_field=DecimalField(max_digits=23, decimal_places=2),
                ),
            )
            .values(
                "agency_name",
                "abbreviation",
                "toptier_code",
                "fiscal_year",
                "fiscal_period",
                "submission_is_quarter",
                "total_dollars_obligated_gtas",
                "total_budgetary_resources",
                "gtas_total_budgetary_resources",
                "total_diff_approp_ocpa_obligated_amounts",
                "recent_publication_date",
                "recent_publication_date_certified",
                "tas_obligations",
                "tas_obligation_not_in_gtas_total",
                "missing_tas_accounts",
                "unlinked_procurement_c_awards",
                "unlinked_assistance_c_awards",
                "unlinked_procurement_d_awards",
                "unlinked_assistance_d_awards",
            )
        )
        return self.format_results(result_list)

    def format_results(self, result_list):
        results = [
            self.format_result(result)
            for result in result_list
            if is_valid_monthly_period(result["fiscal_year"], result["fiscal_period"])
        ]
        results = sorted(
            results,
            key=lambda x: (
                *(
                    (
                        (x["tas_account_discrepancies_totals"][self.pagination.sort_key] is None)
                        == (self.pagination.sort_order == "asc"),
                        x["tas_account_discrepancies_totals"][self.pagination.sort_key],
                    )
                    if (
                        self.pagination.sort_key == "missing_tas_accounts_count"
                        or self.pagination.sort_key == "tas_accounts_total"
                        or self.pagination.sort_key == "tas_obligation_not_in_gtas_total"
                    )
                    else (
                        (x[self.pagination.sort_key] is None) == (self.pagination.sort_order == "asc"),
                        x[self.pagination.sort_key],
                    )
                ),
                x["fiscal_period"],
            ),
            reverse=self.pagination.sort_order == "desc",
        )

        return results

    def format_result(self, result):
        """
        Fields coming from ReportingAgencyOverview are already NULL for periods without
        submissions. Fields coming from other models, such as ReportingAgencyTas, may
        include values even without a submission. For this reason, we initalize those
        fields to NULL in the formatted response, and set them only if a submission exists
        for the record's period.
        """

        formatted_result = {
            "fiscal_year": result["fiscal_year"],
            "fiscal_period": result["fiscal_period"],
            "current_total_budget_authority_amount": None,
            "total_budgetary_resources": None,
            "percent_of_total_budgetary_resources": None,
            "recent_publication_date": result["recent_publication_date"],
            "recent_publication_date_certified": result["recent_publication_date_certified"] is not None,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": None,
                "tas_accounts_total": None,
                "tas_obligation_not_in_gtas_total": None,
                "missing_tas_accounts_count": None,
            },
            "obligation_difference": None,
            "unlinked_contract_award_count": None,
            "unlinked_assistance_award_count": None,
            "assurance_statement_url": None,
        }

        if result["recent_publication_date"]:
            percent_of_total_budgetary_resources = (
                round(result["total_budgetary_resources"] * 100 / result["gtas_total_budgetary_resources"], 2)
                if result["gtas_total_budgetary_resources"] and result["total_budgetary_resources"]
                else None
            )
            unlinked_assistance_award_count = (
                (result["unlinked_assistance_c_awards"] or 0) + (result["unlinked_assistance_d_awards"] or 0)
                if result["unlinked_assistance_c_awards"] or result["unlinked_assistance_d_awards"]
                else None
            )
            unlinked_contract_award_count = (
                (result["unlinked_procurement_c_awards"] or 0) + (result["unlinked_procurement_d_awards"] or 0)
                if result["unlinked_procurement_c_awards"] or result["unlinked_procurement_d_awards"]
                else None
            )
            formatted_result.update(
                {
                    "current_total_budget_authority_amount": result["total_budgetary_resources"],
                    "total_budgetary_resources": result["gtas_total_budgetary_resources"],
                    "percent_of_total_budgetary_resources": percent_of_total_budgetary_resources,
                    "obligation_difference": result["total_diff_approp_ocpa_obligated_amounts"],
                    "unlinked_contract_award_count": unlinked_contract_award_count,
                    "unlinked_assistance_award_count": unlinked_assistance_award_count,
                    "assurance_statement_url": self.create_assurance_statement_url(result),
                }
            )

            formatted_result["tas_account_discrepancies_totals"].update(
                {
                    "gtas_obligation_total": result["total_dollars_obligated_gtas"],
                    "tas_accounts_total": result["tas_obligations"],
                    "tas_obligation_not_in_gtas_total": (result["tas_obligation_not_in_gtas_total"] or 0.0),
                    "missing_tas_accounts_count": result["missing_tas_accounts"],
                }
            )

        return formatted_result
