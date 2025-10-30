from django.utils.functional import cached_property
from rest_framework.request import Request

from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.validator import TinyShield, customize_pagination_with_sort_columns


class PaginationMixin:
    request: Request

    default_sort_column: str
    sortable_columns: list[str]

    @cached_property
    def pagination(self):
        model = customize_pagination_with_sort_columns(self.sortable_columns, self.default_sort_column)
        request_data = (
            TinyShield(model).block(self.request.data)
            if self.request.method == "POST"
            else TinyShield(model).block(self.request.query_params)
        )

        # Use the default sort as a tie-breaker in the case of a different sort provided
        primary_sort_key = request_data.get("sort", self.default_sort_column)
        secondary_sort_key = self.default_sort_column if primary_sort_key != self.default_sort_column else None

        return Pagination(
            page=request_data["page"],
            limit=request_data["limit"],
            lower_limit=(request_data["page"] - 1) * request_data["limit"],
            upper_limit=(request_data["page"] * request_data["limit"]),
            sort_key=primary_sort_key,
            sort_order=request_data["order"],
            secondary_sort_key=secondary_sort_key,
        )
