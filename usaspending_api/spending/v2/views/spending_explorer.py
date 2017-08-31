from datetime import datetime

from decimal import Decimal
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.spending.v2.filters.fy_filter import fy_filter, validate_fy
from usaspending_api.spending.v2.filters.spending_filter import spending_filter
from usaspending_api.spending.v2.views.agency import awarding_top_tier_agency, awarding_agency, awarding_sub_tier_agency
from usaspending_api.spending.v2.views.award import award_category, award
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
                     'program_activity', 'object_class', 'recipient', 'award',
                     'award_category', 'agency', 'agency_top', 'agency_sub']

        if explorer is None:
            raise InvalidParameterException('Missing one or more required request parameters: type')
        elif explorer not in explorers:
            raise InvalidParameterException(
                'Explorer does not have a valid value. '
                'Valid Explorers: budget_function, budget_subfunction, federal_account, '
                'program_activity, object_class, recipient, award, award_category agency, agency_top, agency_sub')

        # Base Queryset
        queryset = FinancialAccountsByProgramActivityObjectClass.objects.all()
        # queryset = FinancialAccountsByAwards.objects.all()
        # Apply filters to explorer type
        if filters is not None:

            # Set end_date
            end_date = None
            for key, value in filters.items():
                if key == 'fy':
                    if value is None:
                        end_date = fy_filter(datetime.now().date())
                    elif value is not None:
                        end_date = validate_fy(value)
                else:
                    end_date = fy_filter(datetime.now().date())

            # Returned filtered queryset
            queryset = spending_filter(queryset, filters)

            # Retrieve explorer type data
            if explorer == 'budget_function':
                results = budget_function(queryset, end_date)
            if explorer == 'budget_subfunction':
                results = budget_subfunction(queryset, end_date)
            if explorer == 'federal_account':
                results = federal_account_budget(queryset, end_date)
            if explorer == 'program_activity':
                results = program_activity(queryset, end_date)
            if explorer == 'object_class':
                results = object_class_budget(queryset, end_date)
            if explorer == 'recipient':
                results = recipient_budget(queryset, end_date)
            if explorer == 'award':
                results = award(queryset, end_date)
            if explorer == 'award_category':
                results = award_category(queryset, end_date)
            if explorer == 'agency':
                results = awarding_agency(queryset, end_date)
            if explorer == 'agency_top':
                results = awarding_top_tier_agency(queryset, end_date)
            if explorer == 'agency_sub':
                results = awarding_sub_tier_agency(queryset, end_date)
            return Response(results)

        # Return explorer type and results if no filter specified
        if filters is None:

            # Set end_date, filter null and NaN - if no filters applied
            end_date = fy_filter(datetime.now().date())
            fiscal_year = datetime.strptime(str(end_date), '%Y-%m-%d').strftime('%Y')
            queryset = queryset.filter(
                submission__reporting_fiscal_year=fiscal_year
            ).exclude(
                obligations_incurred_by_program_object_class_cpe__isnull=True
            )
            for item in queryset.values('obligations_incurred_by_program_object_class_cpe'):
                for key, value in item.items():
                    if value != value or value == Decimal('Inf') or value == Decimal('-Inf'):
                        queryset = queryset.exclude(obligations_incurred_by_program_object_class_cpe=value)

            # Retrieve explorer type data
            if explorer == 'budget_function':
                results = budget_function(queryset, end_date)
            if explorer == 'budget_subfunction':
                results = budget_subfunction(queryset, end_date)
            if explorer == 'federal_account':
                results = federal_account_budget(queryset, end_date)
            if explorer == 'program_activity':
                results = program_activity(queryset, end_date)
            if explorer == 'object_class':
                results = object_class_budget(queryset, end_date)
            if explorer == 'recipient':
                results = recipient_budget(queryset, end_date)
            if explorer == 'award':
                results = award(queryset, end_date)
            if explorer == 'award_category':
                results = award_category(queryset, end_date)
            if explorer == 'agency':
                results = awarding_agency(queryset, end_date)
            if explorer == 'agency_top':
                results = awarding_top_tier_agency(queryset, end_date)
            if explorer == 'agency_sub':
                results = awarding_sub_tier_agency(queryset, end_date)

            return Response(results)
