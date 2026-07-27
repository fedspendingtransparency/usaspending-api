from django.http import HttpResponseBadRequest
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.references.helpers import create_hash
from usaspending_api.references.models import FilterHash


class FilterEndpoint(APIView):
    """Return the hash for a received filters object"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/filter.md"

    def post(self, request: Request, format: str | None = None) -> Response:
        hash_key = create_hash(request.body)

        try:
            fh = FilterHash.objects.get(hash=hash_key)
        except FilterHash.DoesNotExist:
            try:
                # request.data is used because we want json as input
                fh = FilterHash(hash=hash_key, filter=request.data)
                fh.save()
            except Exception:
                return HttpResponseBadRequest("Error storing the filter for future retrieval")

        return Response({"hash": hash_key})


class HashEndpoint(APIView):
    """Return the stored filter object corresponding to the received hash"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/hash.md"

    def post(self, request: Request, format: str | None = None) -> Response | HttpResponseBadRequest:
        if "hash" not in request.data:
            return HttpResponseBadRequest("Missing `hash` key in request body")

        try:
            fh = FilterHash.objects.get(hash=request.data["hash"])
            return Response({"filter": fh.filter})
        except FilterHash.DoesNotExist:
            return HttpResponseBadRequest("A FilterHash Object with that hash does not exist.")
