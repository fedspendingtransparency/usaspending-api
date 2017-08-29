from datetime import datetime

from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.spending.v2.filters.fy_filter import fy_filter
from usaspending_api.spending.v2.filters.spending_filter import spending_filter
from usaspending_api.spending.v2.views.agency_explorer.agency import awarding_agency
from usaspending_api.spending.v2.views.agency_explorer.award import award_category_awarding
from usaspending_api.spending.v2.views.agency_explorer.federal_account import federal_account_awarding
from usaspending_api.spending.v2.views.agency_explorer.object_class import object_class_awarding
from usaspending_api.spending.v2.views.agency_explorer.program_activity import program_activity_awarding
from usaspending_api.spending.v2.views.agency_explorer.recipient import recipient_awarding


class AgencyExplorerViewSet(APIView):

    def post(self, request):
        results = []
        json_request = request.data
        explorer = json_request.get('agency_type')
        filters = json_request.get('filters', None)
        fiscal_year = json_request.get('fiscal_year', None)

        if fiscal_year is None:
            fiscal_year = fy_filter(datetime.now().date())

        explorers = ['agency', 'federal_account', 'program_activity', 'object_class', 'recipients', 'awards']

        if explorer is None:
            raise InvalidParameterException('Missing one or more required request parameters: explorer')
        elif explorer not in explorers:
            raise InvalidParameterException(
                'Explorer does not have a valid value. '
                'Valid Explorers: agency, federal_account, program_activity, object_class, recipients, awards')

        # Filter based on explorer
        if filters is not None:
            queryset = spending_filter(filters)
            queryset = queryset.filter(
                award__period_of_performance_current_end_date=fiscal_year
            ).exclude(obligations_incurred_total_by_award_cpe__isnull=True)
            if explorer == 'agency':
                results = awarding_agency(queryset, fiscal_year)
            if explorer == 'federal_account':
                results = federal_account_awarding(queryset, fiscal_year)
            if explorer == 'program_activity':
                results = program_activity_awarding(queryset, fiscal_year)
            if explorer == 'object_class':
                results = object_class_awarding(queryset, fiscal_year)
            if explorer == 'recipients':
                results = recipient_awarding(queryset, fiscal_year)
            if explorer == 'awards':
                results = award_category_awarding(queryset, fiscal_year)
            return Response(results)

        # Return explorer and results if no filter specified
        if filters is None:
            # Base queryset
            queryset = FinancialAccountsByAwards.objects.all().filter(
                award__period_of_performance_current_end_date=fiscal_year
            ).exclude(obligations_incurred_total_by_award_cpe__isnull=True)
            if explorer == 'agency':
                results = awarding_agency(queryset, fiscal_year)
            if explorer == 'federal_account':
                results = federal_account_awarding(queryset, fiscal_year)
            if explorer == 'program_activity':
                results = program_activity_awarding(queryset, fiscal_year)
            if explorer == 'object_class':
                results = object_class_awarding(queryset, fiscal_year)
            if explorer == 'recipients':
                results = recipient_awarding(queryset, fiscal_year)
            if explorer == 'awards':
                results = award_category_awarding(queryset, fiscal_year)
            return Response(results)
