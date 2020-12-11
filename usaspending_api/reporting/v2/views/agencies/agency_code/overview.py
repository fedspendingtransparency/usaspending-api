from django.db.models import Subquery, OuterRef, DecimalField, Func, F, Q, IntegerField
from rest_framework.response import Response
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from django.utils.functional import cached_property

from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.validator import customize_pagination_with_sort_columns, TinyShield
from usaspending_api.reporting.models import ReportingAgencyOverview, ReportingAgencyTas, ReportingAgencyMissingTas
from usaspending_api.submissions.models import SubmissionAttributes


class AgencyOverview(AgencyBase):
    """Returns an overview of the specified agency's submission data"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/agency_code/overview.md"

    def get(self, request, toptier_code):
        results = self.get_agency_overview()
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        results = results[self.pagination.lower_limit : self.pagination.upper_limit]
        return Response(
            {
                "page_metadata": page_metadata,
                "results": results[: self.pagination.limit],
                "messages": self.standard_response_messages,
            }
        )

    def get_agency_overview(self):
        agency_filters = [
            Q(reporting_fiscal_year=OuterRef("fiscal_year")),
            Q(reporting_fiscal_period=OuterRef("fiscal_period")),
            Q(toptier_code=OuterRef("toptier_code")),
        ]
        result_list = (
            ReportingAgencyOverview.objects.filter(toptier_code=self.toptier_code)
            .annotate(
                recent_publication_date=Subquery(
                    SubmissionAttributes.objects.filter(*agency_filters).values("published_date")
                ),
                recent_publication_date_certified=Subquery(
                    SubmissionAttributes.objects.filter(*agency_filters).values("certified_date")
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
            .values(
                "fiscal_year",
                "fiscal_period",
                "total_dollars_obligated_gtas",
                "total_budgetary_resources",
                "total_diff_approp_ocpa_obligated_amounts",
                "recent_publication_date",
                "recent_publication_date_certified",
                "tas_obligations",
                "missing_tas_accounts",
            )
        )
        return self.format_results(result_list)

    def format_results(self, result_list):
        results = []
        for result in result_list:
            results.append(
                {
                    "fiscal_year": result["fiscal_year"],
                    "fiscal_period": result["fiscal_period"],
                    "current_total_budget_authority_amount": result["total_budgetary_resources"],
                    "recent_publication_date": result["recent_publication_date"],
                    "recent_publication_date_certified": result["recent_publication_date_certified"] is not None,
                    "tas_account_discrepancies_totals": {
                        "gtas_obligation_total": result["total_dollars_obligated_gtas"],
                        "tas_accounts_total": result["tas_obligations"],
                        "missing_tas_accounts_count": result["missing_tas_accounts"],
                    },
                    "obligation_difference": result["total_diff_approp_ocpa_obligated_amounts"],
                }
            )
        results = sorted(
            results,
            key=lambda x: x["tas_account_discrepancies_totals"]["missing_tas_accounts_count"]
            if self.pagination.sort_key == "missing_tas_accounts_count"
            else x[self.pagination.sort_key],
            reverse=self.pagination.sort_order == "desc",
        )
        return results

    @cached_property
    def pagination(self):
        sortable_columns = [
            "fiscal_year",
            "current_total_budget_authority_amount",
            "missing_tas_accounts_total",
            "obligation_difference",
            "recent_publication_date",
            "recent_publication_date_certified",
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
