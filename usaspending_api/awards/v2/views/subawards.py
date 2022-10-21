from copy import deepcopy

from django.db.models import F
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.search.models import SubawardSearch
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield


class SubawardsViewSet(APIView):
    """
    This route sends a request to the backend to retrieve subawards either
    related, optionally, to a specific parent award, or for all parent
    awards if desired.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/subawards.md"

    subaward_lookup = {
        # "Display Name": "database_column"
        "id": "broker_subaward_id",
        "subaward_number": "subaward_number",
        "description": "subaward_description",
        "action_date": "sub_action_date",
        "amount": "subaward_amount",
        "recipient_name": "sub_awardee_or_recipient_legal",
    }

    def _parse_and_validate_request(self, request_dict):
        models = deepcopy(PAGINATION)
        models.append(
            {
                "key": "award_id",
                "name": "award_id",
                "type": "any",
                "models": [{"type": "integer"}, {"type": "text", "text_type": "search"}],
                "optional": True,
                "default": None,
                "allow_nulls": True,
            }
        )
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

        queryset = SubawardSearch.objects.all()

        award_id = request_data["award_id"]
        if award_id is not None:
            award_id_column = "award_id" if type(award_id) is int else "unique_award_key"
            queryset = queryset.filter(**{award_id_column: award_id})

        queryset = queryset.values(*list(self.subaward_lookup.values()))

        # always secondary-sort by PK in case a repeating value (e.g. subaward_number) crosses pages, so suborder isn't abitrary
        if request_data["order"] == "desc":
            queryset = queryset.order_by(
                F(self.subaward_lookup[request_data["sort"]]).desc(nulls_last=True), F("broker_subaward_id").desc()
            )
        else:
            queryset = queryset.order_by(
                F(self.subaward_lookup[request_data["sort"]]).asc(nulls_first=True), F("broker_subaward_id").asc()
            )

        rows = list(queryset[lower_limit : upper_limit + 1])
        return [{k: row[v] for k, v in self.subaward_lookup.items()} for row in rows]

    @cache_response()
    def post(self, request):
        request_data = self._parse_and_validate_request(request.data)
        results = self._business_logic(request_data)
        page_metadata = get_simple_pagination_metadata(len(results), request_data["limit"], request_data["page"])
        response = {"page_metadata": page_metadata, "results": results[: request_data["limit"]]}
        return Response(response)
