from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.views import APIDocumentationView
from rest_framework.response import Response


class SubawardsViewSet(APIDocumentationView):
    """
    endpoint_doc: /subawards/last_updated.md
    """

    @cache_response()
    def get(self, request):
        response = {
            'hasNext': None,
            'hasPrevious': None,
            'results': [],
        }

        return Response(response)
