from django.db.models import Sum
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.references.models.gtas_sf133_balances import GTASSF133Balances

from rest_framework.response import Response
from rest_framework.views import APIView
from usaspending_api.common.exceptions import InvalidParameterException


class TotalBudgetaryResources(APIView):
    @cache_response()
    def get(self, request, format=None):
        fiscal_year = request.query_params.get("fiscal_year")
        fiscal_period = request.query_params.get("fiscal_period")
        gtas = {}

        if fiscal_period:
            if not fiscal_year:
                raise InvalidParameterException("fiscal_period was provided without any fiscal_year.")
            else:
                gtas = (
                    GTASSF133Balances.objects.filter(fiscal_year=fiscal_year)
                    .filter(fiscal_period=fiscal_period)
                    .values("fiscal_year")
                    .values("fiscal_period")
                    .annotate(
                        total_budgetary_resources=Sum("total_budgetary_resources_cpe"),
                    )
                )
        else:
            gtas = (
                GTASSF133Balances.objects.values("fiscal_year")
                .values("fiscal_period")
                .annotate(
                    total_budgetary_resources=Sum("total_budgetary_resources_cpe"),
                )
            )

        print(gtas)

        response = {"results": []}
        response["results"] = [
            {
                "fiscal_year": fiscal_year,
                "fiscal_period": fiscal_period,
                "total_budgetary_resources": float(gtas["total_budgetary_resources"]),
            },
        ]
        return Response(response)
