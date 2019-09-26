from rest_framework.response import Response
from rest_framework.request import Request
from rest_framework.views import APIView

from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.common.helpers.generic_helper import get_pagination
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.serializers import LimitableSerializer
from usaspending_api.references.models import Definition


class DefinitionSerializer(LimitableSerializer):
    class Meta:

        model = Definition
        fields = ["term", "slug", "data_act_term", "plain", "official", "resources"]


class GlossaryViewSet(APIView):
    """
    This view returns paginated glossary terms in a list
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/glossary.md"

    @cache_response()
    def get(self, request: Request) -> Response:
        """
        Accepts only pagination-related query parameters
        """
        models = [
            {"name": "page", "key": "page", "type": "integer", "default": 1, "min": 1},
            {"name": "limit", "key": "limit", "type": "integer", "default": 500, "min": 1, "max": 500},
        ]

        # Can't use the TinyShield decorator (yet) because this is a GET request only
        request_dict = request.query_params
        validated_request_data = TinyShield(models).block(request_dict)
        limit = validated_request_data["limit"]
        page = validated_request_data["page"]

        queryset = Definition.objects.all()
        queryset, pagination = get_pagination(queryset, int(limit), int(page))

        serializer = DefinitionSerializer(queryset, many=True)
        response = {"page_metadata": pagination, "results": serializer.data}

        return Response(response)
