from datetime import datetime

from decimal import Decimal
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.spending.v2.filters.fy_filter import fy_filter, validate_fy
from usaspending_api.spending.v2.filters.spending_filter import spending_filter
from usaspending_api.spending.v2.views.agency import awarding_top_tier_agency
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
                     'program_activity', 'object_class', 'recipients', 'awards', 'agency']

        if explorer is None:
            raise InvalidParameterException('Missing one or more required request parameters: explorer')
        elif explorer not in explorers:
            raise InvalidParameterException(
                'Explorer does not have a valid value. '
                'Valid Explorers: budget_function, budget_subfunction, federal_account, '
                'program_activity, object_class, recipients, awards')

        # Apply filters to explorer type
        if filters is not None:
            # Get filtered queryset
            queryset = spending_filter(filters)
            queryset = queryset.exclude(obligations_incurred_total_by_award_cpe__isnull=True)
            for item in queryset.values('obligations_incurred_total_by_award_cpe'):
                for key, value in item.items():
                    if value == Decimal('Inf') or value == Decimal('-Inf'):
                        queryset.exclude(obligations_incurred_total_by_award_cpe=value)

            # Set fiscal year
            fiscal_year = None
            for key, value in filters.items():
                if key == 'fy':
                    if value is None:
                        fiscal_year = fy_filter(datetime.now().date())
                        queryset = queryset.filter(award__period_of_performance_current_end_date=fiscal_year)
                    elif value is not None:
                        fiscal_year = validate_fy(value)
                        queryset = queryset.filter(award__period_of_performance_current_end_date=fiscal_year)
                else:
                    fiscal_year = fy_filter(datetime.now().date())
                    queryset = queryset.filter(award__period_of_performance_current_end_date=fiscal_year)

            # Retrieve explorer type data
            if explorer == 'budget_function':
                results = budget_function(queryset, fiscal_year)
            if explorer == 'budget_subfunction':
                results = budget_subfunction(queryset, fiscal_year)
            if explorer == 'federal_account':
                results = federal_account_budget(queryset, fiscal_year)
            if explorer == 'program_activity':
                results = program_activity(queryset, fiscal_year)
            if explorer == 'object_class':
                results = object_class_budget(queryset, fiscal_year)
            if explorer == 'recipients':
                results = recipient_budget(queryset, fiscal_year)
            if explorer == 'awards':
                results = award_category(queryset, fiscal_year)
            if explorer == 'agency':
                results = awarding_top_tier_agency(queryset, fiscal_year)
            return Response(results)

        # Return explorer and results if no filter specified
        if filters is None:

            # Set fiscal year and queryset if no filters applied
            fiscal_year = fy_filter(datetime.now().date())
            queryset = FinancialAccountsByAwards.objects.all().filter(
                award__period_of_performance_current_end_date=fiscal_year
            ).exclude(obligations_incurred_total_by_award_cpe__isnull=True,
                      obligations_incurred_total_by_award_cpe='NaN')

            # Retrieve explorer type data
            if explorer == 'budget_function':
                results = budget_function(queryset, fiscal_year)
            if explorer == 'budget_subfunction':
                results = budget_subfunction(queryset, fiscal_year)
            if explorer == 'federal_account':
                results = federal_account_budget(queryset, fiscal_year)
            if explorer == 'program_activity':
                results = program_activity(queryset, fiscal_year)
            if explorer == 'object_class':
                results = object_class_budget(queryset, fiscal_year)
            if explorer == 'recipients':
                results = recipient_budget(queryset, fiscal_year)
            if explorer == 'awards':
                results = award_category(queryset, fiscal_year)
            if explorer == 'agency':
                results = awarding_top_tier_agency(queryset, fiscal_year)
            return Response(results)
