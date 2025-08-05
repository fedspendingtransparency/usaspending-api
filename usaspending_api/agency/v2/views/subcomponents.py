from django.db.models import Case, F, Max, OuterRef, Q, Subquery, Sum, TextField, Value, When
from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any
from usaspending_api.accounts.models.appropriation_account_balances import AppropriationAccountBalances
from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.calculations.file_b import FileBCalculations
from usaspending_api.common.helpers.date_helper import now
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata, sort_with_null_last
from usaspending_api.common.helpers.orm_helpers import ConcatAll
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import BureauTitleLookup
from usaspending_api.submissions.models import SubmissionAttributes


class SubcomponentList(PaginationMixin, AgencyBase):
    """
    Obtain the count of subcomponents (bureaus) for a specific agency in a single
    fiscal year based on GTAS
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/sub_components.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["toptier_code", "fiscal_year"]

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        self.sortable_columns = ["name", "total_obligations", "total_outlays", "total_budgetary_resources"]
        self.default_sort_column = "total_budgetary_resources"
        results = self.format_results(self.get_file_a_queryset(), self.get_file_b_queryset())
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        return Response(
            {
                "toptier_code": self.toptier_code,
                "fiscal_year": self.fiscal_year,
                "results": results[self.pagination.lower_limit : self.pagination.upper_limit],
                "messages": self.standard_response_messages,
                "page_metadata": page_metadata,
            }
        )

    def format_results(self, file_a_response, file_b_response):

        # Combine File A and B Responses
        combined_list_dict = {}

        for row in file_a_response:
            combined_list_dict[row["bureau_info"]] = row

        for row in file_b_response:
            if row["bureau_info"] not in combined_list_dict:
                combined_list_dict[row["bureau_info"]] = row
            else:
                combined_list_dict[row["bureau_info"]].update(row)

        combined_response = [value for key, value in combined_list_dict.items()]

        # Format Combined Response
        results = sort_with_null_last(
            to_sort=[
                {
                    "name": x["bureau_info"].split(";")[0] if x.get("bureau_info") is not None else None,
                    "id": x["bureau_info"].split(";")[1] if x.get("bureau_info") is not None else None,
                    "total_obligations": x["total_obligations"] if x["total_obligations"] else None,
                    "total_outlays": x["total_outlays"] if x["total_outlays"] else None,
                    "total_budgetary_resources": (
                        x["total_budgetary_resources"] if x["total_budgetary_resources"] else None
                    ),
                }
                for x in combined_response
            ],
            sort_key=self.pagination.sort_key,
            sort_order=self.pagination.sort_order,
        )
        return results

    def get_file_a_queryset(self):
        """
        Query Total Budgetary Resources per Bureau from File A for a single Period
        """
        filters, bureau_info_subquery = self.get_common_query_objects("treasury_account_identifier")

        results = (
            (AppropriationAccountBalances.objects.filter(*filters))
            .annotate(bureau_info=bureau_info_subquery)
            .values("bureau_info")
            .annotate(
                total_budgetary_resources=Sum("total_budgetary_resources_amount_cpe"),
            )
            .exclude(bureau_info__isnull=True)
            .values("bureau_info", "total_budgetary_resources")
        )
        return results

    def get_file_b_queryset(self):
        """
        Query Obligations and Outlays per Bureau from File B for a single Period
        """
        filters, bureau_info_subquery = self.get_common_query_objects("treasury_account")
        file_b_calculations = FileBCalculations()
        results = (
            (FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters))
            .annotate(bureau_info=bureau_info_subquery)
            .values("bureau_info")
            .annotate(
                total_obligations=Sum(file_b_calculations.get_obligations()),
                total_outlays=Sum(file_b_calculations.get_outlays()),
            )
            .exclude(bureau_info__isnull=True)
            .values("bureau_info", "total_obligations", "total_outlays")
        )
        return results

    def get_common_query_objects(self, treasury_account_keyword):
        latest = (
            SubmissionAttributes.objects.filter(
                submission_window__submission_reveal_date__lte=now(), reporting_fiscal_year=self.fiscal_year
            )
            .values("reporting_fiscal_year")
            .annotate(max_fiscal_period=Max(F("reporting_fiscal_period")))
            .values("max_fiscal_period")
        )
        filters = [
            Q(**{f"{treasury_account_keyword}__federal_account__parent_toptier_agency": self.toptier_agency}),
            Q(submission__reporting_fiscal_year=self.fiscal_year),
            Q(submission__reporting_fiscal_period=latest[0]["max_fiscal_period"]),
        ]
        bureau_info_subquery = Subquery(
            BureauTitleLookup.objects.filter(
                federal_account_code=OuterRef(f"{treasury_account_keyword}__federal_account__federal_account_code")
            )
            .exclude(federal_account_code__isnull=True)
            .annotate(
                bureau_info=Case(
                    When(
                        federal_account_code__startswith="057",
                        then=ConcatAll(Value("Air Force"), Value(";"), Value("air-force")),
                    ),
                    When(
                        federal_account_code__startswith="021",
                        then=ConcatAll(Value("Army"), Value(";"), Value("army")),
                    ),
                    When(
                        federal_account_code__startswith="017",
                        then=ConcatAll(Value("Navy, Marine Corps"), Value(";"), Value("navy-marine-corps")),
                    ),
                    When(
                        federal_account_code__startswith="097",
                        then=ConcatAll(Value("Defense-wide"), Value(";"), Value("defense-wide")),
                    ),
                    default=ConcatAll(F("bureau_title"), Value(";"), F("bureau_slug")),
                    output_field=TextField(),
                )
            )
            .values("bureau_info")
        )

        return filters, bureau_info_subquery
