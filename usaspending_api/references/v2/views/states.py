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
        # state_data stores one row per FIPS/year. DISTINCT ON with year DESC keeps the latest row.
        from django.db.models.functions import Upper

        rows = (
            StateData.objects.order_by("fips", "-year")
            .distinct("fips")
            .annotate(code_upper=Upper("code"), name_upper=Upper("name"))
            .order_by("code_upper")
            .values("fips", "code_upper", "name_upper")
        )

        results = [
            {
                "fips": row["fips"],
                "code": row["code_upper"] or "",
                "name": row["name_upper"] or "",
            }
            for row in rows
        ]

        return Response({"results": results})
