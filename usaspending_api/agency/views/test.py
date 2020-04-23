from rest_framework.response import Response
from rest_framework.views import APIView


class EndpointTest(APIView):

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/test.md"

    def get(self, request):
        return Response({"status": "up"})
