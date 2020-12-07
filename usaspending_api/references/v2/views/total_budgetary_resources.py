class TotalBudgetaryResources(APIView):

    @cache_response()
    def get(self, request, format=None):
        fiscal_year = request.query_params.get("fiscal_year")
        fiscal_period = request.query_params.get("fiscal_period")

        if fiscal_period and not fiscal_year:
            raise InvalidParameterException("fiscal_period was provided without any fiscal_year.")


