from rest_framework.response import Response

from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin

from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.submissions.models import SubmissionAttributes


class SubmissionHistory(AgencyBase, PaginationMixin):
    """Returns submission history of the specified agency for the specified fiscal year and period"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/submission_history/agency_code/fiscal_year/fiscal_period.md"

    @staticmethod
    def validate_fiscal_period(fiscal_period):
        fiscal_period = int(fiscal_period)
        if fiscal_period < 2 or fiscal_period > 12:
            raise UnprocessableEntityException(f"fiscal_period must be in the range 2-12")

    def get(self, request, toptier_code, fiscal_year, fiscal_period):
        self.fiscal_year = int(fiscal_year)
        self.sortable_columns = [
            "publication_date",
            "certified_date",
        ]
        self.default_sort_column = "publication_date"
        self.validate_fiscal_period(fiscal_period)
        results = (
            SubmissionAttributes.objects.filter(
                toptier_code=toptier_code, reporting_fiscal_year=fiscal_year, reporting_fiscal_period=fiscal_period,
            )
            .order_by("-published_date")
            .values("published_date", "certified_date")
        )
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        results = results[self.pagination.lower_limit : self.pagination.upper_limit]
        return Response(
            {"page_metadata": page_metadata, "results": results, "messages": self.standard_response_messages}
        )
