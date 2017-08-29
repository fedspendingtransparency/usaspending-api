from datetime import datetime

from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.spending.v2.filters.fy_filter import fy_filter
from usaspending_api.spending.v2.filters.spending_filter import spending_filter
from usaspending_api.spending.v2.views.award import award_category
from usaspending_api.spending.v2.views.budget_function import budget_function
from usaspending_api.spending.v2.views.budget_subfunction import budget_subfunction
from usaspending_api.spending.v2.views.federal_account import federal_account_budget
from usaspending_api.spending.v2.views.object_class import object_class_budget
from usaspending_api.spending.v2.views.program_activity import program_activity
from usaspending_api.spending.v2.views.recipient import recipient_budget


class SpendingExplorerViewSet(APIView):

    def post(self, request):
        results = []
        json_request = request.data
        explorer = json_request.get('type')
        filters = json_request.get('filters', None)

        explorers = ['budget_function', 'budget_subfunction', 'federal_account',
                     'program_activity', 'object_class', 'recipients', 'awards']

        if explorer is None:
            raise InvalidParameterException('Missing one or more required request parameters: explorer')
        elif explorer not in explorers:
            raise InvalidParameterException(
                'Explorer does not have a valid value. '
                'Valid Explorers: budget_function, budget_subfunction, federal_account, '
                'program_activity, object_class, recipients, awards')

        # Filter based on explorer
        if filters is not None:
            queryset = spending_filter(filters)
            queryset = queryset.exclude(obligations_incurred_total_by_award_cpe__isnull=True)
            if explorer == 'budget_function':
                results = budget_function(queryset)
            if explorer == 'budget_subfunction':
                results = budget_subfunction(queryset)
            if explorer == 'federal_account':
                results = federal_account_budget(queryset)
            if explorer == 'program_activity':
                results = program_activity(queryset)
            if explorer == 'object_class':
                results = object_class_budget(queryset)
            if explorer == 'recipients':
                results = recipient_budget(queryset)
            if explorer == 'awards':
                results = award_category(queryset)
            return Response(results)

        # Return explorer and results if no filter specified
        if filters is None:
            # Base queryset
            fiscal_year = fy_filter(datetime.now().date())
            queryset = FinancialAccountsByAwards.objects.all().filter(
                award__period_of_performance_current_end_date=fiscal_year
            ).exclude(obligations_incurred_total_by_award_cpe__isnull=True)
            if explorer == 'budget_function':
                results = budget_function(queryset)
            if explorer == 'budget_subfunction':
                results = budget_subfunction(queryset)
            if explorer == 'federal_account':
                results = federal_account_budget(queryset)
            if explorer == 'program_activity':
                results = program_activity(queryset)
            if explorer == 'object_class':
                results = object_class_budget(queryset)
            if explorer == 'recipients':
                results = recipient_budget(queryset)
            if explorer == 'awards':
                results = award_category(queryset)
            return Response(results)
