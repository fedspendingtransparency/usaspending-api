from rest_framework.response import Response
from django.utils.functional import cached_property

from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.validator import customize_pagination_with_sort_columns, TinyShield
from usaspending_api.submissions.models import SubmissionAttributes


class SubmissionHistory(AgencyBase):
    """Returns submission history of the specified agency for the specified fiscal year and period"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/submission_history/agency_code/fiscal_year/fiscal_period.md"

    def get(self, request, toptier_code, fiscal_year, fiscal_period):
        results = SubmissionAttributes.objects.filter(
            toptier_code=toptier_code, reporting_fiscal_year=fiscal_year, reporting_fiscal_period=fiscal_period,
        ).values("published_date", "certified_date")
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        results = results[self.pagination.lower_limit : self.pagination.upper_limit]
        return Response(
            {"page_metadata": page_metadata, "results": results, "messages": self.standard_response_messages}
        )

    @cached_property
    def pagination(self):
        sortable_columns = [
            "publication_date",
            "certified_date",
        ]
        default_sort_column = "publication_date"
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
