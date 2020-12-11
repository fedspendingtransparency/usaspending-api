from django.db.models import Subquery, OuterRef, DecimalField, Func, F, Q, IntegerField
from rest_framework.response import Response
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from django.utils.functional import cached_property

from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.helpers.fiscal_year_helpers import (
    get_final_period_of_quarter,
    calculate_last_completed_fiscal_quarter,
)
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.validator import customize_pagination_with_sort_columns, TinyShield
from usaspending_api.references.models import ToptierAgency, Agency
from usaspending_api.reporting.models import ReportingAgencyOverview, ReportingAgencyTas, ReportingAgencyMissingTas
from usaspending_api.submissions.models import SubmissionAttributes


class AgenciesOverview(AgencyBase):
    """Return list of all agencies and the overview of their spending data for a provided fiscal year and period"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/overview.md"

    def get(self, request):
        results = self.get_agency_overview()
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        results = results[self.pagination.lower_limit : self.pagination.upper_limit]
        return Response(
            {"page_metadata": page_metadata, "results": results, "messages": self.standard_response_messages,}
        )

    def get_agency_overview(self):
        agency_filters = [Q(toptier_code=OuterRef("toptier_code"))]
        if self.filter is not None:
            agency_filters.append(Q(name__icontains=self.filter))
        result_list = (
            ReportingAgencyOverview.objects.filter(fiscal_year=self.fiscal_year, fiscal_period=self.fiscal_period)
            .annotate(
                agency_name=Subquery(ToptierAgency.objects.filter(*agency_filters).values("name")),
                abbreviation=Subquery(ToptierAgency.objects.filter(*agency_filters).values("abbreviation")),
                recent_publication_date=Subquery(
                    SubmissionAttributes.objects.filter(
                        reporting_fiscal_year=OuterRef("fiscal_year"),
                        reporting_fiscal_period=OuterRef("fiscal_period"),
                        toptier_code=OuterRef("toptier_code"),
                    ).values("published_date")
                ),
                recent_publication_date_certified=Subquery(
                    SubmissionAttributes.objects.filter(
                        reporting_fiscal_year=OuterRef("fiscal_year"),
                        reporting_fiscal_period=OuterRef("fiscal_period"),
                        toptier_code=OuterRef("toptier_code"),
                    ).values("certified_date")
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
                    .annotate(count=Func(F("tas_rendering_label"), function="COUNT"))
                    .values("count"),
                    output_field=IntegerField(),
                ),
            )
            .exclude(agency_name__isnull=True)
            .values(
                "agency_name",
                "abbreviation",
                "toptier_code",
                "total_dollars_obligated_gtas",
                "total_budgetary_resources",
                "total_diff_approp_ocpa_obligated_amounts",
                "recent_publication_date",
                "recent_publication_date_certified",
                "tas_obligations",
                "tas_obligation_not_in_gtas_total",
                "missing_tas_accounts",
            )
        )
        return self.format_results(result_list)

    def format_results(self, result_list):
        results = [
            {
                "agency_name": result["agency_name"],
                "abbreviation": result["abbreviation"],
                "agency_code": result["toptier_code"],
                "agency_id": Agency.objects.filter(
                    toptier_agency__toptier_code=result["toptier_code"], toptier_flag=True
                )
                .first()
                .id,
                "current_total_budget_authority_amount": result["total_budgetary_resources"],
                "recent_publication_date": result["recent_publication_date"],
                "recent_publication_date_certified": result["recent_publication_date_certified"] is not None,
                "tas_account_discrepancies_totals": {
                    "gtas_obligation_total": result["total_dollars_obligated_gtas"],
                    "tas_accounts_total": result["tas_obligations"],
                    "tas_obligation_not_in_gtas_total": result["tas_obligation_not_in_gtas_total"] or 0.0,
                    "missing_tas_accounts_count": result["missing_tas_accounts"],
                },
                "obligation_difference": result["total_diff_approp_ocpa_obligated_amounts"],
            }
            for result in result_list
        ]
        results = sorted(
            results,
            key=lambda x: x["tas_account_discrepancies_totals"][self.pagination.sort_key]
            if (
                self.pagination.sort_key == "missing_tas_accounts_count"
                or self.pagination.sort_key == "tas_obligation_not_in_gtas_total"
            )
            else x[self.pagination.sort_key],
            reverse=self.pagination.sort_order == "desc",
        )
        return results

    @cached_property
    def pagination(self):
        sortable_columns = [
            "agency_code",
            "current_total_budget_authority_amount",
            "missing_tas_accounts_count",
            "agency_name",
            "obligation_difference",
            "recent_publication_date",
            "recent_publication_date_certified",
            "tas_obligation_not_in_gtas_total",
        ]
        default_sort_column = "current_total_budget_authority_amount"
        model = customize_pagination_with_sort_columns(sortable_columns, default_sort_column)
        request_data = TinyShield(model).block(self.request.query_params)
        return Pagination(
            page=request_data["page"],
            limit=request_data["limit"],
            lower_limit=(request_data["page"] - 1) * request_data["limit"],
            upper_limit=(request_data["page"] * request_data["limit"]),
            sort_key=request_data.get("sort", default_sort_column),
            sort_order=request_data["order"],
        )

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

    @property
    def filter(self):
        return self.request.query_params.get("filter")
