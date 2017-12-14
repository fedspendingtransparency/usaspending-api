import hashlib
import json

from django.http import HttpResponseBadRequest
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_extensions.cache.decorators import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.awards.models import Award

from usaspending_api.common.api_request_utils import GeoCompleteHandler
from usaspending_api.common.mixins import FilterQuerysetMixin, SuperLoggingMixin
from usaspending_api.common.views import DetailViewSet, AutocompleteView
from usaspending_api.references.models import Location, Agency, LegalEntity, Cfda, Definition, FilterHash
from usaspending_api.references.v1.serializers import LocationSerializer, AgencySerializer, LegalEntitySerializer, \
    CfdaSerializer, DefinitionSerializer, FilterSerializer, HashSerializer


class FilterEndpoint(SuperLoggingMixin, APIView):
    serializer_class = FilterSerializer

    def post(self, request, format=None):
        """return the hash for a json"""
        # get json
        # request.body is used because we want unicode as hash input
        json_req = request.body
        # create hash
        m = hashlib.md5()
        m.update(json_req)
        hash = m.hexdigest().encode('utf8')
        # check for hash in db, store if not in db
        try:
            fh = FilterHash.objects.get(hash=hash)
        except FilterHash.DoesNotExist:
            # store in DB
            try:
                # request.data is used because we want json as input
                fh = FilterHash(hash=hash, filter=request.data)
                fh.save()
            except ValueError:
                return HttpResponseBadRequest("The DB object could not be saved. Value Error Thrown.")
            # added as a catch all. Should never be hit
            except Exception:
                return HttpResponseBadRequest("The DB object could not be saved. Exception Thrown.")
    # # return hash
        return Response({'hash': hash})


class HashEndpoint(SuperLoggingMixin, APIView):
    serializer_class = HashSerializer

    def post(self, request, format=None):
        """return the hash for a json"""
        # get hash
        body_unicode = request.body.decode('utf-8')
        body = json.loads(body_unicode)
        hash = body["hash"]
        # check for hash in db, if not in db
        try:
            fh = FilterHash.objects.get(hash=hash)
            # return filter json
            return Response({'filter': fh.filter})
        except FilterHash.DoesNotExist:
            return HttpResponseBadRequest(
                "The FilterHash Object with that has does not exist. DoesNotExist Error Thrown.")


class LocationEndpoint(SuperLoggingMixin,
                       FilterQuerysetMixin,
                       DetailViewSet):

    """Return an agency"""
    serializer_class = LocationSerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = Location.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class LocationGeoCompleteEndpoint(SuperLoggingMixin, APIView):
    """Return location information."""

    @cache_response()
    def post(self, request, format=None):
        try:
            body_unicode = request.body.decode('utf-8')
            body = json.loads(body_unicode)
            gc = GeoCompleteHandler(body)
            response_data = gc.build_response()
        except Exception as e:
            return Response({"message": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        return Response(response_data)


class AgencyAutocomplete(FilterQuerysetMixin,
                         AutocompleteView):
    """Autocomplete support for agency objects."""
    serializer_class = AgencySerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = Agency.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class AgencyEndpoint(SuperLoggingMixin,
                     FilterQuerysetMixin,
                     DetailViewSet):
    """Return an agency"""
    serializer_class = AgencySerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = Agency.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class CfdaEndpoint(SuperLoggingMixin,
                   FilterQuerysetMixin,
                   DetailViewSet):
    """Return information about CFDA Programs"""
    serializer_class = CfdaSerializer
    lookup_field = "program_number"

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = Cfda.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class RecipientViewSet(SuperLoggingMixin,
                       FilterQuerysetMixin,
                       DetailViewSet):
    """
    Returns information about award recipients and vendors
    """

    serializer_class = LegalEntitySerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = LegalEntity.objects.all().exclude(recipient_unique_id__isnull=True)
        queryset = self.serializer_class.setup_eager_loading(queryset)
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


class RecipientAutocomplete(FilterQuerysetMixin,
                            AutocompleteView):
    """Autocomplete support for legal entity (recipient) objects."""
    serializer_class = LegalEntitySerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = LegalEntity.objects.all().exclude(recipient_unique_id__isnull=True)
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class GlossaryViewSet(SuperLoggingMixin, FilterQuerysetMixin, DetailViewSet):
    """
    This viewset automatically provides `list` and `detail` actions.
    """
    queryset = Definition.objects.all()
    serializer_class = DefinitionSerializer
    lookup_field = 'slug'

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = Definition.objects.all()
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        return filtered_queryset


class AwardAutocompleteViewSet(SuperLoggingMixin, APIView):

    @cache_response()
    # TODO: This only returns one response for each, we have duplicate PIIDs that need to be dealt with in the DB
    def post(self, request):

        json_request = request.data
        search_text = json_request.get('value', None)

        if not search_text:
            raise InvalidParameterException('Missing one or more required request parameters: value')

        all_qs = Award.objects.all()

        piid_qs = all_qs.filter(piid__iexact=search_text)
        parent_award_qs = all_qs.filter(parent_award__piid__iexact=search_text)
        fain_qs = all_qs.filter(fain__iexact=search_text)
        uri_qs = all_qs.filter(uri__iexact=search_text)

        response = {}
        matched_objects = {}
        matched_objects['fain'] = list(fain_qs.values('id', 'fain')[:1])
        matched_objects['parent_award__piid'] = list(parent_award_qs.values('id', 'parent_award__piid')[:1])
        matched_objects['piid'] = list(piid_qs.values('id', 'piid')[:1])
        matched_objects['uri'] = list(uri_qs.values('id', 'uri')[:1])

        response['matched_objects'] = matched_objects

        return Response(
            response
        )


class GlossaryAutocomplete(FilterQuerysetMixin, AutocompleteView):
    """Autocomplete support for legal entity (recipient) objects."""
    serializer_class = DefinitionSerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = Definition.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset
