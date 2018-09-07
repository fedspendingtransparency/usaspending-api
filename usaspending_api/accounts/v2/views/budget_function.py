from django.db.models import Q
from rest_framework.response import Response

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.views import APIDocumentationView


class ListBudgetFunctionViewSet(APIDocumentationView):
    """
    This route sends a request to the backend to retrieve all Budget Functions associated with a TAS, ordered by Budget
        Function code.
    endpoint_doc: /budget_function.md
    """
    @cache_response()
    def get(self, request):
        # Retrieve all Budget Functions, grouped by code and title, ordered by code
        results = TreasuryAppropriationAccount.objects \
            .filter(~Q(budget_function_code=''), ~Q(budget_function_code=None)) \
            .values('budget_function_code', 'budget_function_title') \
            .order_by('budget_function_code').distinct()

        return Response({'results': results})


class ListBudgetSubfunctionViewSet(APIDocumentationView):
    """
    This route sends a request to the backend to retrieve all Budget Subfunctions associated with a TAS, ordered by
        Budget Subfunction code. Can be filtered by Budget Function.
    endpoint_doc: /budget_subfunction.md
    """
    @cache_response()
    def post(self, request):
        # Retrieve all Budget Subfunctions
        queryset = TreasuryAppropriationAccount.objects \
            .filter(~Q(budget_subfunction_code=''), ~Q(budget_subfunction_code=None))

        # Filter by Budget Function, if provided
        budget_function = request.data.get('budget_function', None)
        if budget_function:
            queryset = queryset.filter(budget_function_code=budget_function)

        # Group by code and title, order by code
        results = queryset \
            .values('budget_subfunction_code', 'budget_subfunction_title') \
            .order_by('budget_subfunction_code').distinct()

        return Response({'results': results})
