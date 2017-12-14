from django.db.models import F
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_extensions.cache.decorators import cache_response
from usaspending_api.awards.models import LegalEntity

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import Agency, Cfda, NAICS, PSC
from usaspending_api.references.v1.serializers import AgencySerializer
from usaspending_api.common.mixins import SuperLoggingMixin


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

    # Shared autocomplete...
    def agency_autocomplete(self, request):
        """Search by subtier agencies, return all, with toptiers first"""
        search_text, limit = self.get_request_payload(request)

        queryset = Agency.objects.filter(
            subtier_agency__name__icontains=search_text). \
            order_by('-toptier_flag')

        return Response(
            {'results': AgencySerializer(queryset[:limit], many=True).data}
        )


class AwardingAgencyAutocompleteViewSet(SuperLoggingMixin, BaseAutocompleteViewSet):

    @cache_response()
    def post(self, request):
        return self.agency_autocomplete(request)


class FundingAgencyAutocompleteViewSet(SuperLoggingMixin, BaseAutocompleteViewSet):

    @cache_response()
    def post(self, request):
        return self.agency_autocomplete(request)


class CFDAAutocompleteViewSet(SuperLoggingMixin, BaseAutocompleteViewSet):

    @cache_response()
    def post(self, request):
        """Return CFDA matches by number, title, or name"""
        search_text, limit = self.get_request_payload(request)

        queryset = Cfda.objects.all()

        # Program numbers are 10.4839, 98.2718, etc...
        if search_text.replace('.', '').isnumeric():
            queryset = queryset.filter(program_number__icontains=search_text)
        else:
            title_filter = queryset.filter(program_title__icontains=search_text)
            popular_name_filter = queryset.filter(popular_name__icontains=search_text)
            queryset = title_filter | popular_name_filter

        return Response(
            {'results': list(queryset.values('program_number', 'program_title', 'popular_name')[:limit])}
        )


class NAICSAutocompleteViewSet(SuperLoggingMixin, BaseAutocompleteViewSet):

    @cache_response()
    def post(self, request):
        """Return all NAICS table entries matching the provided search text"""
        search_text, limit = self.get_request_payload(request)

        queryset = NAICS.objects.all()

        # NAICS codes are 111150, 112310, and there are no numeric NAICS descriptions...
        if search_text.isnumeric():
            queryset = queryset.filter(code__icontains=search_text)
        else:
            queryset = queryset.filter(description__icontains=search_text)

        # rename columns...
        queryset = queryset.annotate(naics=F('code'), naics_description=F('description'))

        return Response(
            {'results': list(queryset.values('naics', 'naics_description')[:limit])}
        )


class PSCAutocompleteViewSet(SuperLoggingMixin, BaseAutocompleteViewSet):

    @cache_response()
    def post(self, request):
        """Return all PSC table entries matching the provided search text"""
        search_text, limit = self.get_request_payload(request)

        queryset = PSC.objects.all()

        # PSC codes are 4-digit, but we have some numeric PSC descriptions, so limit to 4...
        if len(search_text) == 4 and queryset.filter(code=search_text.upper()).exists():
            queryset = queryset.filter(code=search_text.upper())
        else:
            queryset = queryset.filter(description__icontains=search_text)

        # rename columns...
        queryset = queryset.annotate(product_or_service_code=F('code'), psc_description=F('description'))

        return Response(
            {'results': list(
                queryset.values('product_or_service_code', 'psc_description')[:limit])}
        )


class RecipientAutocompleteViewSet(SuperLoggingMixin, BaseAutocompleteViewSet):

    @cache_response()
    def post(self, request):
        """Return a list of legal entity IDs whose recipient name contains search_text,
        OR a list od legal entity IDs matching a valid DUNS number.
        Include search_text in response for frontend. """

        # Limit not used here
        search_text, _ = self.get_request_payload(request)

        queryset = LegalEntity.objects.all()

        if len(search_text) < 3:
            raise InvalidParameterException('search_text \'{}\' does not meet '
                                            'the minimum length of 3 characters'.format(search_text))

        is_duns = False
        if len(search_text) == 9 and queryset.filter(recipient_unique_id=search_text).exists():
            is_duns = True

        if is_duns:
            queryset = queryset.filter(recipient_unique_id=search_text)
        else:
            queryset = queryset.filter(recipient_name__icontains=search_text)

        recipients = queryset

        response = {
            'results': {
                'search_text': search_text,
                'recipient_id_list':
                    recipients.values_list('legal_entity_id', flat=True)
            }
        }

        return Response(response)
