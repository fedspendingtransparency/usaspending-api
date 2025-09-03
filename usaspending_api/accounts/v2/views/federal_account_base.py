from django.db.models import Q
from django.utils.functional import cached_property
from rest_framework.request import Request
from rest_framework.views import APIView

from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.validator import TinyShield, customize_pagination_with_sort_columns


class FederalAccountBase(APIView):

    @property
    def federal_account_code(self) -> str:
        return self.kwargs["federal_account_code"]

    def validate_data(self, request_data):
        model = [
            {
                "key": "filters|time_period",
                "name": "time_period",
                "type": "array",
                "array_type": "object",
                "object_keys": {
                    "start_date": {"type": "date"},
                    "end_date": {"type": "date"},
                },
                "optional": True,
            },
            {
                "key": "filters|object_class",
                "name": "object_class",
                "type": "array",
                "array_type": "text",
                "text_type": "raw",
                "optional": True,
            },
            {
                "key": "filters|program_activity",
                "name": "program_activity",
                "type": "array",
                "array_type": "text",
                "text_type": "raw",
                "optional": True,
            },
        ]

        return TinyShield(model).block(request_data)

    def get_filter_query(self, validated_data) -> Q:
        query = Q()
        filters = validated_data.get("filters")
        if filters is not None:
            if "time_period" in filters:
                start_date = filters["time_period"][0]["start_date"]
                end_date = filters["time_period"][0]["end_date"]
                query &= Q(reporting_period_start__gte=start_date, reporting_period_end__lte=end_date)
            if "object_class" in filters:
                query &= Q(
                    Q(object_class__object_class_name__in=filters["object_class"])
                    | Q(object_class__major_object_class_name__in=filters["object_class"])
                )
            if "program_activity" in filters:
                query &= Q(program_activity_reporting_key__code__in=filters["program_activity"]) | Q(
                    program_activity__program_activity_code__in=filters["program_activity"]
                )
            else:
                query &= Q(Q(program_activity_reporting_key__isnull=False) | Q(program_activity__isnull=False))
        return query


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
