import hashlib
import json

from django.http import HttpResponseBadRequest
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.api_request_utils import GeoCompleteHandler
from usaspending_api.common.mixins import FilterQuerysetMixin, SuperLoggingMixin
from usaspending_api.common.views import DetailViewSet, AutocompleteView
from usaspending_api.references.models import Location, Agency, LegalEntity, Cfda, Definition, FilterHash
from usaspending_api.references.v1.serializers import LocationSerializer, AgencySerializer, LegalEntitySerializer, \
    CfdaSerializer, DefinitionSerializer, FilterSerializer, HashSerializer


class FilterEndpoint(APIView):
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


class HashEndpoint(APIView):
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


class LocationGeoCompleteEndpoint(APIView):
    """Return location information."""
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


class GlossaryViewSet(FilterQuerysetMixin, DetailViewSet):
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
