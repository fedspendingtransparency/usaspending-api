from copy import deepcopy

from django.db.models import F
from rest_framework.response import Response

from usaspending_api.awards.models_matviews import SubawardView
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield


class SubawardsViewSet(APIDocumentationView):
    """
    endpoint_doc: /awards/subawards.md
    """

    subaward_lookup = {
        # "Display Name": "database_column"
        "id": "subaward_id",
        "subaward_number": "subaward_number",
        "description": "description",
        "action_date": "action_date",
        "amount": "amount",
        "recipient_name": "recipient_name",
        "award_id": "award_id",
    }

    def _parse_and_validate_request(self, request_dict):
        models = deepcopy(PAGINATION)
        models.append({"key": "award_id", "name": "award_id", "type": "integer",
                      "optional": True, "default": None, "allow_nulls": True})
        for model in models:
            # Change sort to an enum of the desired values
            if model["name"] == "sort":
                model["type"] = "enum"
                model["enum_values"] = list(self.subaward_lookup.keys())
                model["default"] = "subaward_number"

        validated_request_data = TinyShield(models).block(request_dict)
        return validated_request_data

    def _business_logic(self, request_data):
        lower_limit = (request_data["page"] - 1) * request_data["limit"]
        upper_limit = request_data["page"] * request_data["limit"]

        queryset = SubawardView.objects.all().values(*list(self.subaward_lookup.values()))

        if request_data["award_id"] is not None:
            queryset = queryset.filter(award_id=request_data["award_id"])

        if request_data["order"] == "desc":
            queryset = queryset.order_by(F(request_data["sort"]).desc(nulls_last=True))
        else:
            queryset = queryset.order_by(F(request_data["sort"]).asc(nulls_first=True))

        rows = list(queryset[lower_limit:upper_limit + 1])
        return [
            {k: row[v] for k, v in self.subaward_lookup.items() if k != "award_id"}
            for row in rows]

    @cache_response()
    def post(self, request):
        request_data = self._parse_and_validate_request(request.data)
        results = self._business_logic(request_data)
        page_metadata = get_simple_pagination_metadata(len(results), request_data["limit"], request_data["page"])

        response = {
            "page_metadata": page_metadata,
            "results": results[:request_data["limit"]],
        }

        return Response(response)
