from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.references.models import RefCountryCode


class CountriesViewSet(APIView):
    """
    This route returns countries from the ref_country_code table
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/countries.md"

    @cache_response()
    def get(self, request: Request) -> Response:
        # ref_country_code stores country codes and names
        rows = RefCountryCode.objects.values("country_code", "country_name")
        results = sorted(
            (
                {
                    "code": (row["country_code"] or "").upper(),
                    "name": (row["country_name"] or "").upper(),
                }
                for row in rows
            ),
            key=lambda row: row["code"],
        )
        return Response({"results": results})
