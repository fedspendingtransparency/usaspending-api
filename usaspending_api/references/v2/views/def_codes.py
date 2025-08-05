from django.db.models import F
from rest_framework.response import Response
from rest_framework.views import APIView
from usaspending_api.references.models.disaster_emergency_fund_code import DisasterEmergencyFundCode


class DEFCodesViewSet(APIView):
    """This route returns a JSON object describing the DEFC reference data"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/def_codes.md"

    def get(self, request, format=None):
        rows = (
            DisasterEmergencyFundCode.objects.annotate(disaster=F("group_name"))
            .values("code", "public_law", "title", "disaster", "urls")
            .order_by("code")
        )
        for row in rows:
            row["urls"] = row["urls"].split("|") if row["urls"] else None
        return Response({"codes": rows})
