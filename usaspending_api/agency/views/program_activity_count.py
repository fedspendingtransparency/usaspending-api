from rest_framework.response import Response
from rest_framework.views import APIView


class ProgramActivityCount(APIView):
    """
    Obtain the count of program activity categories for a specific agency in a
    single fiscal year
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/agency_id/program_activity/count.md"

    def get(self, request, pk):
        return Response({"count": 7})
