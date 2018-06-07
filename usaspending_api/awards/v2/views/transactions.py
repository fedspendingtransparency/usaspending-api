from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.views import APIDocumentationView
from rest_framework.response import Response


class TransactionViewSet(APIDocumentationView):
    """
    endpoint_doc: /awards/transactions.md
    """

    @cache_response()
    def post(self, request):
        """Return latest updated date for Awards"""

        response = {
            "success": True
        }

        return Response(response)
