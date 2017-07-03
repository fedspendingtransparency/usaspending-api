from django.contrib.postgres.search import TrigramSimilarity
from django.db.models.functions import Greatest
from rest_framework.views import APIView
from rest_framework.response import Response
from usaspending_api.awards.models import LegalEntity
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.serializers_v2.autocomplete import RecipientAutocompleteSerializer


class RecipientAutocompleteViewSet(APIView):

    serializer_class = RecipientAutocompleteSerializer

    def post(self, request):
        """Return all recipients matching the provided search text"""

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

        queryset = LegalEntity.objects.all()

        queryset = queryset.annotate(similarity=Greatest(TrigramSimilarity('recipient_name', search_text),
                                                         TrigramSimilarity('recipient_unique_id', search_text))).\
            order_by('-similarity')

        exact_match_queryset = queryset.filter(similarity=1.0)
        if exact_match_queryset.count() > 0:
            queryset = exact_match_queryset

        return Response({'results': RecipientAutocompleteSerializer(queryset[:limit], many=True).data})
