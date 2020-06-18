from django.utils.functional import cached_property
from rest_framework.views import APIView

from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.validator import customize_pagination_with_sort_columns, TinyShield
from usaspending_api.references.models import DisasterEmergencyFundCode


class DisasterBase(APIView):
    required_filters = ["def_codes"]

    @cached_property
    def filters(self):
        all_def_codes = list(DisasterEmergencyFundCode.objects.values_list("code", flat=True))
        object_keys_lookup = {
            "def_codes": {
                "key": "filter|def_codes",
                "name": "def_codes",
                "type": "array",
                "array_type": "enum",
                "enum_values": all_def_codes,
                "allow_nulls": False,
                "optional": False,
            },
            "spending_type": {
                "key": "filter|spending_type",
                "name": "spending_type",
                "type": "enum",
                "enum_values": ["total", "award"],
                "allow_nulls": False,
                "optional": False,
            },
        }
        model = [object_keys_lookup[key] for key in self.required_filters]
        json_request = TinyShield(model).block(self.request.data)
        return json_request["filter"]

    @property
    def def_codes(self):
        return self.filters["def_codes"]


class SpendingTypeMixin:
    required_filters = ["def_codes", "spending_type"]

    @property
    def spending_type(self):
        return self.filters["spending_type"]


class PaginationMixin:
    @cached_property
    def pagination(self):
        sortable_columns = ["id", "code", "description", "obligation", "outlay", "total_budgetary_resources"]
        default_sort_column = "id"
        model = customize_pagination_with_sort_columns(sortable_columns, default_sort_column)
        request_data = TinyShield(model).block(self.request.data.get("pagination", {}))
        return Pagination(
            page=request_data["page"],
            limit=request_data["limit"],
            lower_limit=(request_data["page"] - 1) * request_data["limit"],
            upper_limit=(request_data["page"] * request_data["limit"]),
            sort_key=request_data.get("sort", "obligated_amount"),
            sort_order=request_data["order"],
        )
