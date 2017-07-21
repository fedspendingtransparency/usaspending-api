from django.contrib.postgres.search import TrigramSimilarity
from django.db.models import F
from django.db.models.functions import Greatest
from rest_framework.response import Response
from rest_framework.views import APIView

<<<<<<< HEAD:usaspending_api/references/views_v2/autocomplete.py
from usaspending_api.awards.models import LegalEntity, TreasuryAppropriationAccount
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import Agency
from usaspending_api.references.serializers import AgencySerializer
=======
from usaspending_api.awards.models import LegalEntity, TreasuryAppropriationAccount, TransactionContract
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import Agency
from usaspending_api.references.v1.serializers import AgencySerializer
>>>>>>> dev:usaspending_api/references/v2/views/autocomplete.py


class BaseAutocompleteViewSet(APIView):

    @staticmethod
    def get_request_payload(request):
        """
            Retrieves all the request attributes needed for the autocomplete endpoints.

            Current attributes:
            * search_text : string to search for
            * limit : number of items to return
        """

        json_request = request.data

        # retrieve search_text from request
        search_text = json_request.get('search_text', None)

        try:
            limit = int(json_request.get('limit', 10))
        except ValueError:
            raise InvalidParameterException('Limit request parameter is not a valid, positive integer')

        # required query parameters were not provided
        if not search_text:
            raise InvalidParameterException('Missing one or more required request parameters: search_text')

        return search_text, limit

    def agency_autocomplete_response(self, request):
        order_list = ['-toptier_flag', '-similarity']
        search_text, limit = self.get_request_payload(request)

        queryset = Agency.objects.filter(subtier_agency__isnull=False)

        queryset = queryset.annotate(similarity=TrigramSimilarity('subtier_agency__name', search_text)). \
            order_by(*order_list)

        exact_match_queryset = queryset.filter(similarity=1.0)
        if exact_match_queryset.count() > 0:
            queryset = exact_match_queryset

        return Response({'results': AgencySerializer(queryset[:limit], many=True).data})


class AwardingAgencyAutocompleteViewSet(BaseAutocompleteViewSet):

    def post(self, request):
        """Return all awarding agencies matching the provided search text"""
        return self.agency_autocomplete_response(request)


class BudgetFunctionAutocompleteViewSet(BaseAutocompleteViewSet):

    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""

        search_text, limit = self.get_request_payload(request)

        queryset = TreasuryAppropriationAccount.objects.all()

        # Filter based on search text
        response = {}

        function_results = queryset.annotate(similarity=TrigramSimilarity('budget_function_title', search_text)).\
            distinct().order_by('-similarity')

        subfunction_results = queryset.annotate(similarity=TrigramSimilarity('budget_subfunction_title', search_text)).\
            distinct().order_by('-similarity')

        function_exact_match_queryset = function_results.filter(similarity=1.0)
        if function_exact_match_queryset.count() > 0:
            function_results = function_exact_match_queryset

        subfunction_exact_match_queryset = subfunction_results.filter(similarity=1.0)
        if subfunction_exact_match_queryset.count() > 0:
            subfunction_results = subfunction_exact_match_queryset

        function_titles = list(function_results.values_list('budget_function_title', flat=True)[:limit])
        subfunction_titles = list(subfunction_results.values_list('budget_subfunction_title', flat=True)[:limit])

        response['results'] = {'budget_function_title': function_titles,
                               'budget_subfunction_title': subfunction_titles}

        response['counts'] = {'budget_function_title': len(function_titles),
                              'budget_subfunction_title': len(subfunction_titles)}

        return Response(response)


class FundingAgencyAutocompleteViewSet(BaseAutocompleteViewSet):

    def post(self, request):
        """Return all funding agencies matching the provided search text"""
        return self.agency_autocomplete_response(request)


class NAICSAutocompleteViewSet(BaseAutocompleteViewSet):

    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""

        search_text, limit = self.get_request_payload(request)

        # get relevant TransactionContracts
        queryset = TransactionContract.objects.filter(naics__isnull=False, naics_description__isnull=False)
        # Filter based on search text
        response = {}

<<<<<<< HEAD:usaspending_api/references/views_v2/autocomplete.py
=======
        queryset = queryset.annotate(similarity=Greatest(
            TrigramSimilarity('naics', search_text),
            TrigramSimilarity('naics_description', search_text)))\
            .distinct().order_by('-similarity')

        naics_exact_match_queryset = queryset.filter(similarity=1.0)
        if naics_exact_match_queryset.count() > 0:
            queryset = naics_exact_match_queryset

        results_set = list(queryset.values('naics', 'naics_description')[:limit])
        response['results'] = results_set

        return Response(response)


class RecipientAutocompleteViewSet(BaseAutocompleteViewSet):

>>>>>>> dev:usaspending_api/references/v2/views/autocomplete.py
    def post(self, request):
        """Return all Parents and Recipients matching the provided search text"""

        search_text, limit = self.get_request_payload(request)

        # Return Recipients with valid id entries
        queryset = LegalEntity.objects.exclude(
            recipient_unique_id__isnull=True).exclude(
            recipient_unique_id__exact='')

        # Filter based on search text
        response = {}

        # Search and filter for Parent Recipients
        parents = queryset.annotate(
            similarity=Greatest(
                TrigramSimilarity('recipient_name', search_text),
                TrigramSimilarity('parent_recipient_unique_id', search_text))
        ).distinct().order_by('-similarity').filter(
            parent_recipient_unique_id__in=F('recipient_unique_id'))[:limit]

        # Search and filter for Recipients, excluding Parent Recipients
        recipients = queryset.annotate(
            similarity=Greatest(
                TrigramSimilarity('recipient_name', search_text),
                TrigramSimilarity('recipient_unique_id', search_text))
        ).distinct().order_by('-similarity').exclude(
            parent_recipient_unique_id__in=F('recipient_unique_id'))[:limit]

        response['results'] = {
            'parent_recipient': [
                parents.values('legal_entity_id',
                               'recipient_name',
                               'parent_recipient_unique_id')[:limit]],
            'recipient': [
                recipients.values('legal_entity_id',
                                  'recipient_name',
                                  'recipient_unique_id')[:limit]]}

        response['counts'] = {'parent_recipient': len(parents),
                              'recipient': len(recipients)}

        return Response(response)



<<<<<<< HEAD:usaspending_api/references/views_v2/autocomplete.py
=======
        column_names = ['legal_entity_id', 'recipient_name', 'recipient_unique_id']

        return Response({'results': list(queryset.values(*column_names)[:limit])})


class ToptierAgencyAutocompleteViewSet(BaseAutocompleteViewSet):

    def post(self, request):
        """Return all toptier agencies matching the provided search text"""

        json_request = request.data

        # retrieve search_text from request
        search_text = json_request.get('search_text', None)
        limit = json_request.get('limit')

        # required query parameters were not provided
        if not search_text:
            raise InvalidParameterException('Missing one or more required request parameters: search_text')

        # if there's a limit present, convert to an int. otherwise everything will be returned
        if limit:
            try:
                limit = int(limit)
            except ValueError:
                raise InvalidParameterException('Limit request parameter is not a valid, positive integer')

        queryset = Agency.objects.filter(toptier_flag=True)

        queryset = queryset.annotate(similarity=TrigramSimilarity('toptier_agency__name', search_text)).\
            order_by('-similarity')

        exact_match_queryset = queryset.filter(similarity=1.0)
        if exact_match_queryset.count() > 0:
            queryset = exact_match_queryset

        queryset = queryset.annotate(agency_id=F('id'), agency_name=F('toptier_agency__name'))

        column_names = ['agency_id', 'agency_name']

        results = list(queryset.values(*column_names)[:limit]) if limit else list(queryset.values(*column_names))

        return Response({'results': results})
>>>>>>> dev:usaspending_api/references/v2/views/autocomplete.py
