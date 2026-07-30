import json

from django.http import HttpResponse, HttpResponseBadRequest
from pydantic import ValidationError
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.references.helpers import create_hash
from usaspending_api.references.models import FilterHash
from usaspending_api.references.pydantic_models import FilterHashRequest, HashLookupRequest


class FilterEndpoint(APIView):
    """Return the hash for a received filters object"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/filter.md"
    # Cap request size at 512KB.
    MAX_REQUEST_SIZE = 512 * 1024

    def post(self, request: Request, format: str | None = None) -> Response | HttpResponse:
        if len(request.body) > self.MAX_REQUEST_SIZE:
            return HttpResponse(
                "Request body exceeds maximum allowed size",
                status=413,
                content_type="text/plain",
            )

        try:
            # Validate the request data using Pydantic.
            FilterHashRequest(**request.data)
            hash_key = create_hash(request.body)

            if not FilterHash.objects.filter(hash=hash_key).exists():
                fh = FilterHash(hash=hash_key, filter=request.data)
                fh.save()

            return Response({"hash": hash_key})
        except (ValidationError, Exception) as e:
            if isinstance(e, ValidationError):
                error_response = json.dumps({"error": "Invalid request format", "details": e.errors()})
            else:
                error_response = "Error storing the filter for future retrieval"
            return HttpResponseBadRequest(error_response)


class HashEndpoint(APIView):
    """Return the stored filter object corresponding to the received hash"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/hash.md"

    def post(self, request: Request, format: str | None = None) -> Response | HttpResponseBadRequest:
        try:
            validated_request = HashLookupRequest(**request.data)
        except (ValidationError, Exception) as e:
            error_message = (
                json.dumps({"error": "Invalid request format", "details": e.errors()})
                if isinstance(e, ValidationError)
                else f"Error parsing request: {str(e)}"
            )
            return HttpResponseBadRequest(error_message)

        try:
            fh = FilterHash.objects.get(hash=validated_request.hash)
        except FilterHash.DoesNotExist:
            return HttpResponseBadRequest("A FilterHash object with that hash does not exist.")

        return Response({"filter": fh.filter})
