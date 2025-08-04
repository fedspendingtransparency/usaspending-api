from django.db.models import DecimalField, F, Func, IntegerField, OuterRef, Q, Subquery, Value
from rest_framework.response import Response

from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.references.models import Agency, ToptierAgencyPublishedDABSView
from usaspending_api.reporting.models import ReportingAgencyMissingTas, ReportingAgencyOverview, ReportingAgencyTas
from usaspending_api.submissions.models import SubmissionAttributes


class AgenciesOverview(PaginationMixin, AgencyBase):
    """Return list of all agencies and the overview of their spending data for a provided fiscal year and period"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/overview.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year", "fiscal_period", "filter"]

    def get(self, request):
        self.sortable_columns = [
            "toptier_code",
            "current_total_budget_authority_amount",
            "missing_tas_accounts_count",
            "tas_accounts_total",
            "agency_name",
            "obligation_difference",
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
        agency_filters = []
        if self.filter is not None:
            agency_filters.append(Q(name__icontains=self.filter) | Q(abbreviation__icontains=self.filter))
        reporting_filters = [
            Q(toptier_code=OuterRef("toptier_code")),
            Q(fiscal_year=self.fiscal_year),
            Q(fiscal_period=self.fiscal_period),
        ]
        result_list = (
            ToptierAgencyPublishedDABSView.objects.filter(*agency_filters)
            .annotate(
                agency_name=F("name"),
                fiscal_year=Value(self.fiscal_year, output_field=IntegerField()),
                fiscal_period=Value(self.fiscal_period, output_field=IntegerField()),
                current_total_budget_authority_amount=Subquery(
                    ReportingAgencyOverview.objects.filter(*reporting_filters).values("total_budgetary_resources")
                ),
                obligation_difference=Subquery(
                    ReportingAgencyOverview.objects.filter(*reporting_filters).values(
                        "total_diff_approp_ocpa_obligated_amounts"
                    )
                ),
                total_dollars_obligated_gtas=Subquery(
                    ReportingAgencyOverview.objects.filter(*reporting_filters).values("total_dollars_obligated_gtas")
                ),
                unlinked_contract_award_count=Subquery(
                    ReportingAgencyOverview.objects.filter(*reporting_filters)
                    .annotate(
                        unlinked_contract_award_count=F("unlinked_procurement_c_awards")
                        + F("unlinked_procurement_d_awards")
                    )
                    .values("unlinked_contract_award_count"),
                    output_field=IntegerField(),
                ),
                unlinked_assistance_award_count=Subquery(
                    ReportingAgencyOverview.objects.filter(*reporting_filters)
                    .annotate(
                        unlinked_assistance_award_count=F("unlinked_assistance_c_awards")
                        + F("unlinked_assistance_d_awards")
                    )
                    .values("unlinked_assistance_award_count"),
                    output_field=IntegerField(),
                ),
                recent_publication_date=Subquery(
                    SubmissionAttributes.objects.filter(
                        reporting_fiscal_year=self.fiscal_year,
                        reporting_fiscal_period=self.fiscal_period,
                        toptier_code=OuterRef("toptier_code"),
                    ).values("published_date")
                ),
                recent_publication_date_certified=Subquery(
                    SubmissionAttributes.objects.filter(
                        reporting_fiscal_year=self.fiscal_year,
                        reporting_fiscal_period=self.fiscal_period,
                        toptier_code=OuterRef("toptier_code"),
                    ).values("certified_date")
                ),
                submission_is_quarter=Subquery(
                    SubmissionAttributes.objects.filter(
                        reporting_fiscal_year=self.fiscal_year,
                        reporting_fiscal_period=self.fiscal_period,
                        toptier_code=OuterRef("toptier_code"),
                    ).values("quarter_format_flag")
                ),
                tas_accounts_total=Subquery(
                    ReportingAgencyTas.objects.filter(
                        fiscal_year=self.fiscal_year,
                        fiscal_period=self.fiscal_period,
                        toptier_code=OuterRef("toptier_code"),
                    )
                    .annotate(the_sum=Func(F("appropriation_obligated_amount"), function="SUM"))
                    .values("the_sum"),
                    output_field=DecimalField(max_digits=23, decimal_places=2),
                ),
                tas_obligation_not_in_gtas_total=Subquery(
                    ReportingAgencyMissingTas.objects.filter(
                        fiscal_year=self.fiscal_year,
                        fiscal_period=self.fiscal_period,
                        toptier_code=OuterRef("toptier_code"),
                    )
                    .annotate(the_sum=Func(F("obligated_amount"), function="SUM"))
                    .values("the_sum"),
                    output_field=DecimalField(max_digits=23, decimal_places=2),
                ),
                missing_tas_accounts_count=Subquery(
                    ReportingAgencyMissingTas.objects.filter(
                        fiscal_year=self.fiscal_year,
                        fiscal_period=self.fiscal_period,
                        toptier_code=OuterRef("toptier_code"),
                    )
                    .exclude(obligated_amount=0)
                    .annotate(count=Func(F("tas_rendering_label"), function="COUNT"))
                    .values("count"),
                    output_field=IntegerField(),
                ),
            )
            .values(
                "agency_name",
                "abbreviation",
                "toptier_code",
                "total_dollars_obligated_gtas",
                "current_total_budget_authority_amount",
                "obligation_difference",
                "recent_publication_date",
                "recent_publication_date_certified",
                "tas_accounts_total",
                "tas_obligation_not_in_gtas_total",
                "missing_tas_accounts_count",
                "fiscal_year",
                "fiscal_period",
                "submission_is_quarter",
                "unlinked_contract_award_count",
                "unlinked_assistance_award_count",
            )
        )

        # If we're sorting by `agency_name` then lowercase every name so that agencies like "Department of the Interior"
        #   and "Department of the Treasury" are in the correct place and not just placed at the end of the list.
        if self.pagination.sort_key == "agency_name":
            formatted_results = sorted(
                self.format_results(result_list),
                key=lambda x: x["agency_name"].lower(),
                reverse=(self.pagination.sort_order == "desc"),
            )
        else:
            formatted_results = sorted(
                self.format_results(result_list),
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
                    x["toptier_code"],
                ),
                reverse=(self.pagination.sort_order == "desc"),
            )

        return formatted_results

    def format_results(self, result_list):
        agencies = {
            a["toptier_agency__toptier_code"]: a["id"]
            for a in Agency.objects.filter(toptier_flag=True).values("toptier_agency__toptier_code", "id")
        }
        results = [self.format_result(result, agencies) for result in result_list]
        return results

    def format_result(self, result, agencies):
        """
        Fields coming from ReportingAgencyOverview are already NULL, for periods without
        submissions. Fields coming from other models, such as ReportingAgencyTas, may
        include values even without a submission. For this reason, we initalize those
        fields to NULL in the formatted response, and set them only if a submission exists
        for the record's period.
        """

        formatted_result = {
            "agency_name": result["agency_name"],
            "abbreviation": result["abbreviation"],
            "toptier_code": result["toptier_code"],
            "agency_id": agencies.get(result["toptier_code"]),
            "current_total_budget_authority_amount": None,
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
            formatted_result.update(
                {
                    "current_total_budget_authority_amount": result["current_total_budget_authority_amount"],
                    "obligation_difference": result["obligation_difference"],
                    "unlinked_contract_award_count": result["unlinked_contract_award_count"],
                    "unlinked_assistance_award_count": result["unlinked_assistance_award_count"],
                    "assurance_statement_url": self.create_assurance_statement_url(result),
                }
            )
            formatted_result["tas_account_discrepancies_totals"].update(
                {
                    "gtas_obligation_total": result["total_dollars_obligated_gtas"],
                    "tas_accounts_total": result["tas_accounts_total"],
                    "tas_obligation_not_in_gtas_total": (result["tas_obligation_not_in_gtas_total"] or 0.0),
                    "missing_tas_accounts_count": result["missing_tas_accounts_count"],
                }
            )
        return formatted_result
