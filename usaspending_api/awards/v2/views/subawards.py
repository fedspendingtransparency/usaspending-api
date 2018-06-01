from copy import deepcopy

from django.db.models import F
from rest_framework.response import Response

from usaspending_api.awards.models_matviews import SubawardView
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.core.validator.pagination import PAGINATION
from usaspending_api.core.validator.tinyshield import TinyShield
from usaspending_api.common.exceptions import InvalidParameterException


class SubawardsViewSet(APIDocumentationView):
    """
    endpoint_doc: /subawards/last_updated.md
    """

    subaward_lookup = {
        # "Display Name": "database_column"
        "id": "id",
        "subaward_number": "subaward_number",
        "description": "description",
        "action_date": "action_date",
        "amount": "amount",
        "recipient_name": "recipient_name",
    }

    @cache_response()
    def post(self, request):
        models = deepcopy(PAGINATION)
        models.append({"key": "award_id", "name": "award_id", "type": "integer",
                      "optional": True, "default": None, "allow_nulls": True})
        for model in models:
            if model["name"] == "sort":
                model["default"] = "subaward_number"

        request_data = TinyShield(models).block(request.data)
        lower_limit = (request_data["page"] - 1) * request_data["limit"]
        upper_limit = request_data["page"] * request_data["limit"]
        values = list(self.subaward_lookup.values())
        if request_data["sort"] not in values:
            raise InvalidParameterException("Sort value not found in fields: {}".format(request_data["sort"]))

        queryset = SubawardView.objects.all().values(*values)

        if request_data["award_id"] is not None:
            queryset = queryset.filter(award_id=request_data["award_id"])

        if request_data["order"] == "desc":
            queryset = queryset.order_by(F(request_data["sort"]).desc(nulls_last=True))
        else:
            queryset = queryset.order_by(F(request_data["sort"]).asc(nulls_first=True))

        from usaspending_api.common.helpers.generic_helper import generate_raw_quoted_query
        print('=======================================')
        print(request.path)
        print(generate_raw_quoted_query(queryset))

        rows = list(queryset[lower_limit:upper_limit + 1])
        results = [
            {k: row[v] for k, v in self.subaward_lookup.items()}
            for row in rows]

        page_metadata = get_simple_pagination_metadata(len(results), request_data["limit"], request_data["page"])

        response = {
            "page_metadata": page_metadata,
            "results": results[:request_data["limit"]],
        }

        return Response(response)
