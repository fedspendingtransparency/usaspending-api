from django.contrib.postgres.search import TrigramSimilarity
from django.db.models import F
from django.db.models.functions import Greatest
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_extensions.cache.decorators import cache_response
from usaspending_api.awards.models import LegalEntity, TransactionFPDS
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import Agency, Cfda, NAICS, PSC
from usaspending_api.references.v1.serializers import AgencySerializer


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


class AwardingAgencyAutocompleteViewSet(BaseAutocompleteViewSet):

    @cache_response()
    def post(self, request):
        """Search by subtier agencies, return all, with toptiers first"""
        search_text, limit = self.get_request_payload(request)

        queryset = Agency.objects.filter(subtier_agency__name__icontains=search_text). \
            order_by('-toptier_flag')

        return Response(
            {'results': AgencySerializer(queryset[:limit], many=True).data}
        )


class FundingAgencyAutocompleteViewSet(BaseAutocompleteViewSet):

    @cache_response()
    def post(self, request):
        """Return only toptier agencies"""
        search_text, limit = self.get_request_payload(request)

        queryset = Agency.objects.filter(toptier_flag=True, toptier_agency__name__icontains=search_text)

        return Response(
            {'results': AgencySerializer(queryset[:limit], many=True).data}
        )


class CFDAAutocompleteViewSet(BaseAutocompleteViewSet):

    @cache_response()
    def post(self, request):
        """Return CFDA matches by number, title, or name"""
        search_text, limit = self.get_request_payload(request)

        queryset = Cfda.objects.all()

        # Program numbers are 10.4839, 98.2718, etc...
        if search_text.replace('.', '').isnumeric():
            queryset = queryset.filter(program_number__icontains=search_text)
        else:
            queryset = queryset.annotate(similarity=Greatest(
                TrigramSimilarity('program_title', search_text),
                TrigramSimilarity('popular_name', search_text))) \
                .distinct().order_by('-similarity')

        return Response(
            {'results': list(queryset.values('program_number', 'program_title', 'popular_name')[:limit])}
        )


class NAICSAutocompleteViewSet(BaseAutocompleteViewSet):

    @cache_response()
    def post(self, request):
        """Return all NAICS table entries matching the provided search text"""
        search_text, limit = self.get_request_payload(request)

        queryset = NAICS.objects.all()

        # CFDA codes are 111150, 112310, and there are no numeric NAICS descriptions...
        if search_text.isnumeric():
            queryset = queryset.filter(code__icontains=search_text)
        else:
            queryset = queryset.annotate(similarity=TrigramSimilarity('description', search_text)). \
                order_by('-similarity')

        # rename columns...
        queryset = queryset.annotate(naics=F('code'), naics_description=F('description'))

        return Response(
            {'results': list(queryset.values('naics', 'naics_description')[:limit])}
        )


class PSCAutocompleteViewSet(BaseAutocompleteViewSet):

    @cache_response()
    def post(self, request):
        """Return all PSC table entires matching the provided search text"""
        search_text, limit = self.get_request_payload(request)

        queryset = PSC.objects.all()

        # CFDA codes are 4-digit, but we have some numeric PSC descriptions, so limit to 4...
        if len(search_text) == 4 and queryset.filter(code=search_text.upper()).exists():
            queryset = queryset.filter(code=search_text.upper())
        else:
            queryset = queryset.annotate(similarity=TrigramSimilarity('description', search_text)). \
                order_by('-similarity')

        # rename columns...
        queryset = queryset.annotate(product_or_service_code=F('code'), psc_description=F('description'))

        return Response(
            {'results': list(
                queryset.values('product_or_service_code', 'psc_description')[:limit])}
        )


class RecipientAutocompleteViewSet(BaseAutocompleteViewSet):

    @cache_response()
    def post(self, request):
        """Return all Parents and Recipients matching the provided search text"""

        search_text, limit = self.get_request_payload(request)

        queryset = LegalEntity.objects.all()

        response = {}

        is_duns = False
        if search_text.isnumeric() and len(search_text) == 9:
            is_duns = True

        if is_duns:
            queryset = queryset.filter(recipient_unique_id=search_text)
        else:
            queryset = queryset.filter(recipient_name__icontains=search_text)

        parents = queryset.filter(
            parent_recipient_unique_id__in=F('recipient_unique_id'))

        recipients = queryset.exclude(
            parent_recipient_unique_id__in=F('recipient_unique_id'))

        # Format response
        response['results'] = {
            'parent_recipient':
               parents.values('legal_entity_id',
                              'recipient_name',
                              'parent_recipient_unique_id')[:limit],
            'recipient':
                recipients.values('legal_entity_id',
                                  'recipient_name',
                                  'recipient_unique_id')[:limit]}

        return Response(response)
