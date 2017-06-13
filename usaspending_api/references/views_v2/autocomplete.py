from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.references.serializers_v2.autocomplete import BudgetFunctionAutocompleteSerializer
from usaspending_api.common.views import DetailViewSet, AutocompleteView
from usaspending_api.common.exceptions import InvalidParameterException
from django.db.models import F, Sum, Q
from rest_framework.decorators import api_view
from rest_framework.response import Response
from usaspending_api.common.exceptions import status


@api_view(['GET'])
def budget_function_autocomplete_view(request):
    """Return all award spending by award type for a given fiscal year and agency id"""

    json_request = request.query_params

    # retrieve search_text from request
    search_text = json_request.get('search_text', None)

    # required query parameters were not provided
    if not search_text:
        return Response({'message': 'Missing one or more required query parameters: search_text'},
                        status=status.HTTP_400_BAD_REQUEST)

    queryset = TreasuryAppropriationAccount.objects.all()

    # Filter based on search text
    response = {}
    function_queryset = queryset.values('budget_function_title').\
        filter(Q(budget_function_title__icontains=search_text)).distinct()
    subfunction_queryset = queryset.values('budget_subfunction_title').\
        filter(Q(budget_subfunction_title__icontains=search_text)).distinct()

    function_titles = function_queryset.values_list('budget_function_title', flat=True)
    subfunction_titles = subfunction_queryset.values_list('budget_subfunction_title', flat=True)

    response['results'] = {'budget_function_title': function_titles,
                           'budget_subfunction_title': subfunction_titles}

    response['counts'] = {'budget_function_title': len(function_titles),
                          'budget_subfunction_title': len(subfunction_titles)}

    return Response(response)
