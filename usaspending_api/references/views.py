from django.shortcuts import render
from rest_framework import status, viewsets
from rest_framework.views import APIView
from rest_framework.response import Response
from usaspending_api.common.api_request_utils import GeoCompleteHandler, AutoCompleteHandler
from usaspending_api.references.models import Location, Agency, LegalEntity, CFDAProgram, Definition
from usaspending_api.references.serializers import LocationSerializer, AgencySerializer, LegalEntitySerializer, CfdaSerializer, DefinitionSerializer
from usaspending_api.common.mixins import FilterQuerysetMixin, SuperLoggingMixin
from usaspending_api.common.views import DetailViewSet, AutocompleteView
import json


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
        return filtered_queryset


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
        queryset = CFDAProgram.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class RecipientAutocomplete(FilterQuerysetMixin,
                            AutocompleteView):
    """Autocomplete support for legal entity (recipient) objects."""
    serializer_class = LegalEntitySerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = LegalEntity.objects.all().exclude(recipient_unique_id__isnull=True)
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        return filtered_queryset


class GuideViewSet(FilterQuerysetMixin, DetailViewSet):
    """
    This viewset automatically provides `list` and `detail` actions.
    """
    queryset = Definition.objects.all()
    serializer_class = DefinitionSerializer    
    lookup_field = 'term'    
    
    def get_queryset(self):
        """Return the view's queryset."""
        queryset = Definition.objects.all()
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset
