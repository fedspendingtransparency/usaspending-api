from rest_framework.response import Response
from rest_framework.views import APIView


class Placeholder(APIView):
    """Placeholder"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/recipient/duns.md"

    def get(self, request):
        return Response({"status": "success"})
