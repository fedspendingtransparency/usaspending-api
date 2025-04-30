from django.db.models import Sum, Q, F, Max
from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any, List
from usaspending_api.accounts.models.appropriation_account_balances import AppropriationAccountBalances
from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.calculations.file_b import FileBCalculations
from usaspending_api.common.helpers.date_helper import now
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import BureauTitleLookup
from usaspending_api.submissions.models import SubmissionAttributes


class BureauFederalAccountList(PaginationMixin, AgencyBase):
    """
    Obtain the list of federal accounts and treasury accounts for a specific agency in a
    single fiscal year based on whether or not that federal account/treasury account has ever
    been submitted in File B.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/sub_components/bureau_slug.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year", "agency_type"]

    @property
    def bureau_slug(self):
        return self.kwargs["bureau_slug"]

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        self.sortable_columns = ["name", "id", "total_obligations", "total_outlays", "total_budgetary_resources"]
        self.default_sort_column = "total_obligations"
        results = sorted(
            self.get_federal_account_list(),
            key=lambda x: x.get(self.pagination.sort_key),
            reverse=self.pagination.sort_order == "desc",
        )
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        totals = self.get_totals(results)
        return Response(
            {
                "toptier_code": self.toptier_code,
                "bureau_slug": self.bureau_slug,
                "fiscal_year": self.fiscal_year,
                "totals": totals,
                "page_metadata": page_metadata,
                "results": results[self.pagination.lower_limit : self.pagination.upper_limit],
                "messages": self.standard_response_messages,
            }
        )

    def get_totals(self, results) -> dict:
        totals = {
            "total_obligations": sum(x["total_obligations"] for x in results),
            "total_outlays": sum(x["total_outlays"] for x in results),
            "total_budgetary_resources": sum(x["total_budgetary_resources"] for x in results),
        }
        return totals

    def get_federal_account_list(self) -> List[dict]:
        # Retreive List of Federal Accounts to Query
        fed_account_filter = Q(bureau_slug=self.bureau_slug)
        if self.bureau_slug == "air-force":
            fed_account_filter = Q(federal_account_code__startswith="057")
        elif self.bureau_slug == "army":
            fed_account_filter = Q(federal_account_code__startswith="021")
        elif self.bureau_slug == "navy-marine-corps":
            fed_account_filter = Q(federal_account_code__startswith="017")
        elif self.bureau_slug == "defense-wide":
            fed_account_filter = Q(federal_account_code__startswith="097")

        federal_accounts = [
            x["federal_account_code"]
            for x in BureauTitleLookup.objects.filter(fed_account_filter).values("federal_account_code")
        ]

        file_a_response = self.get_file_a_accounts(federal_accounts)
        file_b_response = self.get_file_b_accounts(federal_accounts)

        # Combine File A and B Responses
        combined_list_dict = {}

        for row in file_a_response:
            combined_list_dict[row["account_code"]] = row

        for row in file_b_response:
            if row["account_code"] not in combined_list_dict:
                combined_list_dict[row["account_code"]] = row
            else:
                combined_list_dict[row["account_code"]].update(row)

        combined_response = [value for key, value in combined_list_dict.items()]

        # Format Combined Response
        results = [
            {
                "name": x["name"],
                "id": x["account_code"],
                "total_obligations": x["total_obligations"],
                "total_outlays": x["total_outlays"],
                "total_budgetary_resources": x["total_budgetary_resources"],
            }
            for x in combined_response
        ]
        return results

    def get_file_a_accounts(self, federal_accounts):
        """
        Query Total Budgetary Resources per Bureau from File A for a single Period
        """
        filters, annotations = self.get_common_query_objects(federal_accounts, "treasury_account_identifier")

        return (
            AppropriationAccountBalances.objects.filter(*filters)
            .annotate(**annotations)
            .values("name", "account_code")
            .annotate(
                total_budgetary_resources=Sum("total_budgetary_resources_amount_cpe"),
            )
            .values("name", "account_code", "total_budgetary_resources")
        )

    def get_file_b_accounts(self, federal_accounts):
        """
        Query Obligations and Outlays per Bureau from File B for a single Period
        """
        filters, annotations = self.get_common_query_objects(federal_accounts, "treasury_account")
        file_b_calculations = FileBCalculations()
        return (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters)
            .annotate(**annotations)
            .values("name", "account_code")
            .annotate(
                total_obligations=Sum(file_b_calculations.get_obligations()),
                total_outlays=Sum(file_b_calculations.get_outlays()),
            )
            .values("name", "account_code", "total_obligations", "total_outlays")
        )

    def get_common_query_objects(self, federal_accounts, treasury_account_keyword):
        latest = (
            SubmissionAttributes.objects.filter(
                submission_window__submission_reveal_date__lte=now(), reporting_fiscal_year=self.fiscal_year
            )
            .values("reporting_fiscal_year")
            .annotate(max_fiscal_period=Max(F("reporting_fiscal_period")))
            .values("max_fiscal_period")
        )
        filters = [
            Q(
                **{
                    f"{treasury_account_keyword}__federal_account__parent_toptier_agency_id": self.toptier_agency.toptier_agency_id
                }
            ),
            Q(**{f"{treasury_account_keyword}__federal_account__federal_account_code__in": federal_accounts}),
            Q(submission__reporting_fiscal_year=self.fiscal_year),
            Q(submission__reporting_fiscal_period=latest[0]["max_fiscal_period"]),
        ]

        annotations = {
            "name": F(f"{treasury_account_keyword}__federal_account__account_title"),
            "account_code": F(f"{treasury_account_keyword}__federal_account__federal_account_code"),
        }

        return filters, annotations
