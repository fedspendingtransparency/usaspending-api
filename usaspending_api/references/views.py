from django.shortcuts import render
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from usaspending_api.common.api_request_utils import DataQueryHandler, GeoCompleteHandler, AutoCompleteHandler
from usaspending_api.references.models import Location, Agency, LegalEntity
from usaspending_api.references.serializers import LocationSerializer, AgencySerializer, LegalEntitySerializer
from usaspending_api.common.mixins import FilterQuerysetMixin, ResponseMetadatasetMixin
from usaspending_api.common.views import AggregateView, DetailViewSet
import json


class LocationEndpoint(APIView):
    """Return location information."""
    def post(self, request, geocomplete=False, format=None):
        try:
            body_unicode = request.body.decode('utf-8')
            body = json.loads(body_unicode)
            response_data = None
            if not geocomplete:
                dq = DataQueryHandler(Location, LocationSerializer, body)
                response_data = dq.build_response()
            else:
                gc = GeoCompleteHandler(body)
                response_data = gc.build_response()
        except Exception as e:
            return Response({"message": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        return Response(response_data)


class AgencyAutocomplete(APIView):
    """Autocomplete support for agency objects."""
    def post(self, request, format=None):
        try:
            body_unicode = request.body.decode('utf-8')
            body = json.loads(body_unicode)
            return Response(AutoCompleteHandler.handle(Agency.objects.all(), body, serializer=AgencySerializer))
        except Exception as e:
            return Response({"message": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class AgencyEndpoint(FilterQuerysetMixin,
                     ResponseMetadatasetMixin,
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


class RecipientAutocomplete(APIView):
    """Autocomplete support for legal entity (recipient) objects."""
    def post(self, request, format=None):
        try:
            body_unicode = request.body.decode('utf-8')
            body = json.loads(body_unicode)
            return Response(AutoCompleteHandler.handle(LegalEntity.objects.all(), body, LegalEntitySerializer))
        except Exception as e:
            return Response({"message": str(e)}, status=status.HTTP_400_BAD_REQUEST)
