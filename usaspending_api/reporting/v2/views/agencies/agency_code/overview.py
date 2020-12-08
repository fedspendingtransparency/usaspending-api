from rest_framework.response import Response
from rest_framework.views import APIView


class AgencyOverview(APIView):
    """Placeholder"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/agency_code/overview.md"

    def get(self, request):
        return Response({"status": "success"})
