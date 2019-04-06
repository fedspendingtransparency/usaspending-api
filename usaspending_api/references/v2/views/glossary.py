from rest_framework.response import Response
from rest_framework.request import Request

from usaspending_api.common.helpers.generic_helper import get_pagination
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.common.serializers import LimitableSerializer
from usaspending_api.references.models import Definition


class DefinitionSerializer(LimitableSerializer):

    class Meta:

        model = Definition
        fields = ['term', 'slug', 'data_act_term', 'plain', 'official', 'resources']


class GlossaryViewSet(APIDocumentationView):
    """
    This view returns paginated glossary terms in a list
    """

    @cache_response()
    def get(self, request: Request) -> Response:
        """
        Accepts only pagination-related query parameters
        """

        get_request = request.query_params
        limit = get_request.get('limit', 500)
        page = get_request.get('page', 1)

        queryset = Definition.objects.all()
        queryset, pagination = get_pagination(queryset, int(limit), int(page))

        serializer = DefinitionSerializer(queryset, many=True)
        response = {
            "page_metadata":pagination,
            "results":serializer.data
        }

        return Response(response)








