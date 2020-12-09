from usaspending_api.common.cache_decorator import cache_response

from rest_framework.response import Response
from rest_framework.views import APIView
from usaspending_api.common.exceptions import InvalidParameterException


class TotalBudgetaryResources(APIView):
    @cache_response()
    def get(self, request, format=None):
        fiscal_year = request.query_params.get("fiscal_year")
        fiscal_period = request.query_params.get("fiscal_period")

        if fiscal_period and not fiscal_year:
            raise InvalidParameterException("fiscal_period was provided without any fiscal_year.")
