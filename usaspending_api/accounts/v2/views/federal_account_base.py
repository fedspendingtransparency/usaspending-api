from django.db.models import Q
from rest_framework.views import APIView

from usaspending_api.common.validator import TinyShield


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
                time_period_query = Q()
                for period in filters["time_period"]:
                    start_date = period["start_date"]
                    end_date = period["end_date"]
                    time_period_query |= Q(reporting_period_start__gte=start_date, reporting_period_end__lte=end_date)
                query &= time_period_query
            if "object_class" in filters:
                query &= Q(
                    Q(object_class__object_class_name__in=filters["object_class"])
                    | Q(object_class__major_object_class_name__in=filters["object_class"])
                )
            if "program_activity" in filters:
                query &= Q(program_activity_reporting_key__code__in=filters["program_activity"]) | Q(
                    program_activity__program_activity_code__in=filters["program_activity"]
                )
        return query

    def get_program_activity_query(self, validated_data) -> Q:
        query = self.get_filter_query(validated_data)
        filters = validated_data.get("filters")
        if filters is not None and "program_activity" not in filters:
            query &= Q(Q(program_activity_reporting_key__isnull=False) | Q(program_activity__isnull=False))

        return query
