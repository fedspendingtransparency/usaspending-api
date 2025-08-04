from django.db.models import Sum
from rest_framework.response import Response
from rest_framework.views import APIView
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.references.models.gtas_sf133_balances import GTASSF133Balances
from usaspending_api.common.helpers.generic_helper import get_account_data_time_period_message


class TotalBudgetaryResources(APIView):
    """
    This route sends a request to the backend to retrieve GTAS totals by FY/FP.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/total_budgetary_resources.md"

    @cache_response()
    def get(self, request):
        model = [
            {
                "key": "fiscal_year",
                "name": "fiscal_year",
                "type": "integer",
                "min": 2017,
                "optional": True,
                "default": None,
                "allow_nulls": True,
            },
            {
                "key": "fiscal_period",
                "name": "fiscal_period",
                "type": "integer",
                "min": 2,
                "max": 12,
                "optional": True,
                "default": None,
                "allow_nulls": True,
            },
        ]
        validated = TinyShield(model).block(request.query_params)

        fiscal_year = validated.get("fiscal_year", None)
        fiscal_period = validated.get("fiscal_period", None)
        gtas_queryset = GTASSF133Balances.objects.values("fiscal_year", "fiscal_period")

        if fiscal_period:
            if not fiscal_year:
                raise InvalidParameterException("fiscal_period was provided without fiscal_year.")
            else:
                gtas_queryset = gtas_queryset.filter(fiscal_year=fiscal_year, fiscal_period=fiscal_period)

        elif fiscal_year:
            gtas_queryset = gtas_queryset.filter(fiscal_year=fiscal_year)

        results = gtas_queryset.annotate(total_budgetary_resources=Sum("total_budgetary_resources_cpe")).order_by(
            "-fiscal_year", "-fiscal_period"
        )

        return Response(
            {
                "results": list(results),
                "messages": [get_account_data_time_period_message()] if not fiscal_year or fiscal_year < 2017 else [],
            }
        )
