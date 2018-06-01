from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.views import APIDocumentationView
from rest_framework.response import Response
from usaspending_api.awards.models import Subaward
from usaspending_api.core.validator.tinyshield import TinyShield
from usaspending_api.core.validator.pagination import PAGINATION
from copy import deepcopy
from django.db.models import F


class SubawardsViewSet(APIDocumentationView):
    """
    endpoint_doc: /subawards/last_updated.md
    """
    SubAwardResult = {
        "id": None,
        "subaward_number": None,
        "description": None,
        "action_date": None,
        "amount": None,
        "recipient_name": None,
    }

    subaward_lookup = {
        # "Display Name": "database_column"
        "id": "id",
        "subaward_number": "subaward_number",
        "description": "description",
        "action_date": "action_date",
        "amount": "amount",
        "recipient_name": "recipient__recipient_name",
    }

    @cache_response()
    def post(self, request):
        models = deepcopy(PAGINATION)
        models.append({'key': 'award_id', 'name': 'award_id', 'type': 'integer', 'optional': True})
        for model in models:
            if model["name"] == "sort":
                model["default"] = "id"

        validated_params = TinyShield(models).block(request.data)
        print('=' * 80)
        print(request.path)
        print(validated_params)
        qs = Subaward.objects.all()
        values = list(self.subaward_lookup.values())
        print(values)
        print()
        print(*values)
        lower_limit = (validated_params["page"] - 1) * validated_params["limit"]
        upper_limit = validated_params["page"] * validated_params["limit"]

        if validated_params["order"] == "desc":
            qs = qs.order_by(F(validated_params["sort"]).desc(nulls_last=True))
        else:
            qs = qs.order_by(F(validated_params["sort"]).asc(nulls_last=True))

        qs = qs.values(*values)[lower_limit:upper_limit + 1]

        from usaspending_api.common.helpers.generic_helper import generate_raw_quoted_query
        print('=======================================')
        print(request.path)
        print(generate_raw_quoted_query(qs))

        has_next = len(qs) > validated_params["limit"]

        results = []
        for row in qs[:validated_params["limit"]]:
            r = deepcopy(self.SubAwardResult)
            for k, v in self.subaward_lookup.items():
                r[k] = row[v]
            results.append(r)

        response = {
            'limit': validated_params["limit"],
            'hasNext': has_next,
            'hasPrevious': validated_params["page"] > 0,
            'results': results,
        }

        return Response(response)
