from django.db.models import Subquery, OuterRef, DecimalField, Func, F, Q, IntegerField, Value
from rest_framework.response import Response

from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from django.utils.functional import cached_property

from usaspending_api.common.helpers.fiscal_year_helpers import (
    get_final_period_of_quarter,
    calculate_last_completed_fiscal_quarter,
)
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.references.models import ToptierAgency, Agency
from usaspending_api.reporting.models import ReportingAgencyOverview, ReportingAgencyTas, ReportingAgencyMissingTas
from usaspending_api.submissions.models import SubmissionAttributes


class AgenciesOverview(AgencyBase, PaginationMixin):
    """Return list of all agencies and the overview of their spending data for a provided fiscal year and period"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/overview.md"

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
            ToptierAgency.objects.account_agencies("awarding")
            .filter(*agency_filters)
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

        if self.pagination.sort_order == "desc":
            result_list = result_list.order_by(F(self.pagination.sort_key).desc(nulls_last=True))
        else:
            result_list = result_list.order_by(F(self.pagination.sort_key).asc(nulls_last=True))

        return self.format_results(result_list)

    def format_results(self, result_list):
        agencies = {
            a["toptier_agency__toptier_code"]: a["id"]
            for a in Agency.objects.filter(toptier_flag=True).values("toptier_agency__toptier_code", "id")
        }
        results = [
            {
                "agency_name": result["agency_name"],
                "abbreviation": result["abbreviation"],
                "toptier_code": result["toptier_code"],
                "agency_id": agencies.get(result["toptier_code"]),
                "current_total_budget_authority_amount": result["current_total_budget_authority_amount"],
                "recent_publication_date": result["recent_publication_date"],
                "recent_publication_date_certified": result["recent_publication_date_certified"] is not None,
                "tas_account_discrepancies_totals": {
                    "gtas_obligation_total": result["total_dollars_obligated_gtas"],
                    "tas_accounts_total": result["tas_accounts_total"],
                    "tas_obligation_not_in_gtas_total": result["tas_obligation_not_in_gtas_total"] or 0.0,
                    "missing_tas_accounts_count": result["missing_tas_accounts_count"],
                },
                "obligation_difference": result["obligation_difference"],
                "unlinked_contract_award_count": result["unlinked_contract_award_count"],
                "unlinked_assistance_award_count": result["unlinked_assistance_award_count"],
                "assurance_statement_url": self.create_assurance_statement_url(result)
                if result["recent_publication_date"]
                else None,
            }
            for result in result_list
        ]
        return results

    @cached_property
    def fiscal_period(self):
        """
        This is the fiscal period we want to limit our queries to when querying CPE values for
        self.fiscal_year.  If it's prior to Q1 submission window close date, we will return
        quarter 1 anyhow and just show what we have (which will likely be incomplete).
        """
        return self.request.query_params.get(
            "fiscal_period", get_final_period_of_quarter(calculate_last_completed_fiscal_quarter(self.fiscal_year)) or 3
        )
