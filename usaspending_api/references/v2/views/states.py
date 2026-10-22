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

        # First: Get distinct rows with uppercase annotations
        rows = (
            StateData.objects.order_by("fips", "-year")
            .distinct("fips")
            .annotate(code_upper=Upper("code"), name_upper=Upper("name"))
            .values("fips", "code_upper", "name_upper")
        )

        # Second: Sort in Python (since we can't re-order after DISTINCT ON in same query)
        results = sorted(
            [
                {
                    "fips": row["fips"],
                    "code": row["code_upper"] or "",
                    "name": row["name_upper"] or "",
                }
                for row in rows
            ],
            key=lambda row: row["code"],
        )

        return Response({"results": results})
