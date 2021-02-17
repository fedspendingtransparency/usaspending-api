from django.db.models import F
from rest_framework.response import Response

from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin

from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.submissions.models import SubmissionAttributes


class SubmissionHistory(PaginationMixin, AgencyBase):
    """Returns submission history of the specified agency for the specified fiscal year and period"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/toptier_code/fiscal_year/fiscal_period/submission_history.md"

    def get(self, request, toptier_code, fiscal_year, fiscal_period):
        self.fiscal_year = int(fiscal_year)
        self.sortable_columns = ["publication_date", "certification_date"]
        self.default_sort_column = "publication_date"
        self.validate_fiscal_period({"fiscal_period": int(fiscal_period)})
        record = list(
            SubmissionAttributes.objects.filter(
                toptier_code=toptier_code, reporting_fiscal_year=fiscal_year, reporting_fiscal_period=fiscal_period
            )
            .values_list("history", flat=True)
        )

        print(record)
        # if len(record) == 0:
        #     return Response()

        results = sorted(
            [
                {"publication_date": row["published_date"], "certification_date": row["certified_date"]}
                for row in record[0]
            ],
            key=lambda x: x[self.pagination.sort_key],
            reverse=self.pagination.sort_order == "desc",
        )

        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        results = results[self.pagination.lower_limit : self.pagination.upper_limit]
        return Response(
            {"page_metadata": page_metadata, "results": results, "messages": self.standard_response_messages}
        )
