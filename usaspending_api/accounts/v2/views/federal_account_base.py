from typing import List

from django.utils.functional import cached_property
from rest_framework.request import Request
from rest_framework.views import APIView

from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.validator import TinyShield, customize_pagination_with_sort_columns


class FederalAccountBase(APIView):

    params_to_validate: List[str]

    @property
    def federal_account_code(self) -> str:
        return self.kwargs["federal_account_code"]

    def _validate_params(self, param_values, params_to_validate=None):
        all_models = [
            {
                "key": "time_period",
                "name": "time_period",
                "type": "array",
                "optional": True
            },
            {
                "key": "object_class",
                "name": "object_class",
                "type": "array",
                "optional": True
            },
            {
                "key": "program_activity",
                "name": "program_activity",
                "type": "array",
                "optional": True
            }
        ]

        return TinyShield(all_models)


    def filter(self):
        return self._validate_params(self)


class PaginationMixin:
    request: Request

    default_sort_column: str
    sortable_columns: list[str]

    @cached_property
    def pagination(self):
        model = customize_pagination_with_sort_columns(self.sortable_columns, self.default_sort_column)
        request_data = TinyShield(model).block(self.request.query_params)

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
