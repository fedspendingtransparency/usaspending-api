from django.shortcuts import render
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from usaspending_api.common.api_request_utils import DataQueryHandler, GeoCompleteHandler
from usaspending_api.references.models import Location
from usaspending_api.references.serializers import LocationSerializer
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
