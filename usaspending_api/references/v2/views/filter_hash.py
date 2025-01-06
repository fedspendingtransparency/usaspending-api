import hashlib

from django.http import HttpResponseBadRequest
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.references.models import FilterHash


class FilterEndpoint(APIView):
    """Return the hash for a received filters object"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/filter.md"

    @staticmethod
    def create_hash(payload):
        """
        Create a MD5 hash from a Python dict
        (Some tomfoolery here due to Python's handling of byte strings)
        """
        m = hashlib.md5(usedforsecurity=False)
        m.update(payload)
        hash_key = m.hexdigest().encode("utf8")
        if len(str(hash_key)) > 2 and str(hash_key)[:2] == "b'":
            hash_key = str(hash_key)[2:-1]
        return hash_key

    def post(self, request, format=None):
        hash_key = self.create_hash(request.body)

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

    def post(self, request, format=None):
        if "hash" not in request.data:
            return HttpResponseBadRequest("Missing `hash` key in request body")

        try:
            fh = FilterHash.objects.get(hash=request.data["hash"])
            return Response({"filter": fh.filter})
        except FilterHash.DoesNotExist:
            return HttpResponseBadRequest("A FilterHash Object with that hash does not exist.")
