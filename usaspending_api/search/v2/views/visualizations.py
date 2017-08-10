from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.awards.v2.filters.transaction import transaction_filter


class SpendingOverTimeVisualizationViewSet(APIView):

    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""

        json_request = request.data
        group = json_request.get('group', None)
        filters = json_request.get('filters', None)

        if group is None:
            raise InvalidParameterException('Missing one or more required request parameters: group')
        if filters is None:
            raise InvalidParameterException('Missing one or more required request parameters: filters')
        potential_groups = ["quarter", "fiscal_year", "month", "fy", "q", "m"]
        if group not in potential_groups:
            raise InvalidParameterException('group does not have a valid value')

        queryset = transaction_filter(filters)
        # Filter based on search text
        # print(queryset.query)
        response = {'group': group, 'results': []}

        # filter queryset by time
        queryset = queryset.sort_by("award__period_of_performance_start_date")
        oldest_date = queryset.last().award.period_of_performance_start_date

        fiscal_years_in_results = {}  # list of time_period objects ie {"fy": "2017", "quarter": "3"} : 1000

        for trans in queryset:
            transaction_year = trans.action_data.spilt('-')[0]
            key = {}
            if group == "fy" or group == "fiscal_year":
                key = {"fiscal_year": transaction_year}
            elif group == "m" or group == 'month':
                key = {"fiscal_year": transaction_year, "month": "00"}
            else: #quarter
                key = {"fiscal_year": transaction_year, "quarter": "1"}

            if key in fiscal_years_in_results.items():
                fiscal_years_in_results[key] += trans.federal_action_obligation
            else:
                fiscal_years_in_results[key] = trans.federal_action_obligation

        results = []
        # [{
        # "time_period": {"fy": "2017", "quarter": "3"},
        # 	"aggregated_amount": "200000000"
        # }]
        for key, value in fiscal_years_in_results:
            result = {"time_period": key, "aggregated_ammount": value}
            results.append(result)
        response['results'] = results


        return Response(result)
