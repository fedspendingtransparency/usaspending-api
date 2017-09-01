from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.spending.v2.filters.type_filter import type_filter


class SpendingExplorerViewSet(APIView):

    def post(self, request):

        json_request = request.data
        explorer = json_request.get('type')
        filters = json_request.get('filters', None)

        # Returned filtered queryset
        results = type_filter(explorer, filters)

        return Response(results)
