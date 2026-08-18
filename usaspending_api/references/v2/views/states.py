from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.recipient.models import StateData


class StatesViewSet(APIView):
    """
    This route returns the latest U.S. States, districts and territories from the state_data table
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/states.md"

    @cache_response()
    def get(self, request: Request) -> Response:
        # state_data stores one row per FIPS/year. DISTINCT ON flips with year DESC keeps the latest row.
        rows = StateData.objects.order_by("fips", "-year").distinct("fips").values("fips", "code", "name")
        results = sorted(
            (
                {
                    "fips": row["fips"],
                    "code": (row["code"] or "").upper(),
                    "name": (row["name"] or "").upper(),
                }
                for row in rows
            ),
            key=lambda row: row["code"],
        )
        return Response({"results": results})
