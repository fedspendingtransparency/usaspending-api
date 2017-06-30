from django.contrib.postgres.search import TrigramSimilarity
from django.db.models.functions import Greatest
from django.db.models import Q
from rest_framework.views import APIView
from rest_framework.response import Response
from usaspending_api.awards.models import LegalEntity
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.serializers_v2.autocomplete import RecipientAutocompleteSerializer


class BaseAutocompleteViewSet(APIView):
    # SearchRank ranks a non-matching result with 1e-20
    SEARCH_SIMILARITY_THRESHOLD = 0.03


class RecipientAutocompleteViewSet(BaseAutocompleteViewSet):

    serializer_class = RecipientAutocompleteSerializer

    def get_queryset(self):
        """Return all award spending by award type for a given fiscal year and agency id"""

        json_request = self.request.data

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
            filter(Q(recipient_name__trigram_similar=search_text) |
                   Q(recipient_unique_id__trigram_similar=search_text),
                   similarity__gt=self.SEARCH_SIMILARITY_THRESHOLD).\
            values('legal_entity_id', 'recipient_name', 'recipient_unique_id', 'similarity').\
            order_by('-similarity')[:limit]

        return queryset
