from django.contrib.postgres.search import SearchQuery, SearchRank, SearchVector
from rest_framework.views import APIView
from rest_framework.response import Response
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.common.exceptions import status


class BaseAutocompleteViewSet(APIView):
    # SearchRank ranks a non-matching result with 1e-20
    SEARCH_RANK_NO_MATCH = 0.00000000000000000001


class BudgetFunctionAutocompleteViewSet(BaseAutocompleteViewSet):

    def post(self, request):
        """Return all award spending by award type for a given fiscal year and agency id"""

        json_request = request.data

        # retrieve search_text from request
        search_text = json_request.get('search_text', None)

        try:
            limit = int(json_request.get('limit', 10))
        except ValueError:
            return Response({'message': 'Limit request parameter is not a valid, positive integer'},
                            status=status.HTTP_400_BAD_REQUEST)

        # required query parameters were not provided
        if not search_text:
            return Response({'message': 'Missing one or more required request parameters: search_text'},
                            status=status.HTTP_400_BAD_REQUEST)

        queryset = TreasuryAppropriationAccount.objects.all()

        # Filter based on search text
        response = {}

        function_vector = SearchVector('budget_function_title')
        subfunction_vector = SearchVector('budget_subfunction_title')
        query = SearchQuery(search_text)

        function_results = queryset.annotate(rank=SearchRank(function_vector, query)).values('budget_function_title'). \
            distinct().filter(rank__gt=self.SEARCH_RANK_NO_MATCH).order_by('-rank')

        subfunction_results = queryset.annotate(rank=SearchRank(subfunction_vector, query)). \
            values('budget_subfunction_title').distinct().filter(rank__gt=self.SEARCH_RANK_NO_MATCH).order_by('-rank')

        function_titles = function_results.values_list('budget_function_title', flat=True)[:limit]
        subfunction_titles = subfunction_results.values_list('budget_subfunction_title', flat=True)[:limit]

        response['results'] = {'budget_function_title': function_titles,
                               'budget_subfunction_title': subfunction_titles}

        response['counts'] = {'budget_function_title': len(function_titles),
                              'budget_subfunction_title': len(subfunction_titles)}

        return Response(response)

