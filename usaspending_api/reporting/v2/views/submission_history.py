from rest_framework.response import Response

from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from usaspending_api.common.exceptions import NoDataFoundException
from usaspending_api.common.helpers.date_helper import now
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.submissions.models import SubmissionAttributes


class SubmissionHistory(PaginationMixin, AgencyBase):
    """Returns submission history of the specified agency for the specified fiscal year and period"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/toptier_code/fiscal_year/fiscal_period/submission_history.md"

    def get(self, request, toptier_code, fiscal_year, fiscal_period):
        validated_params = self.validated_url_params
        fiscal_year = validated_params["fiscal_year"]
        fiscal_period = validated_params["fiscal_period"]
        self.sortable_columns = ["publication_date", "certification_date"]
        self.default_sort_column = "publication_date"
        record = list(
            SubmissionAttributes.objects.filter(
                toptier_code=toptier_code,
                reporting_fiscal_year=fiscal_year,
                reporting_fiscal_period=fiscal_period,
                submission_window_id__submission_reveal_date__lte=now(),
            ).values_list("history", flat=True)
        )

        if len(record) == 0:
            raise NoDataFoundException("No Agency Account Submission History records match the provided parameters")

        # Convoluted list comprehension and sort to
        #  A) construct the dict list
        #  B) add secondary sort key and handle nulls in `certification_date` for sorting
        results = sorted(
            [
                {"publication_date": row["published_date"], "certification_date": row["certified_date"]}
                for row in record[0]
            ],
            key=lambda x: (
                x["publication_date"]
                if self.pagination.sort_key == "publication_date"
                else (x["certification_date"] or "", x["publication_date"])
            ),
            reverse=self.pagination.sort_order == "desc",
        )

        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        results = results[self.pagination.lower_limit : self.pagination.upper_limit]
        return Response(
            {"page_metadata": page_metadata, "results": results, "messages": self.standard_response_messages}
        )
