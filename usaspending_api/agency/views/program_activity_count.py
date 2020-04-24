from rest_framework.response import Response
from rest_framework.views import APIView


class ProgramActivityCount(APIView):
    """blah"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/id/program_activity_count.md"

    def get(self, request, parameters):
        return Response({"status": "up"})
