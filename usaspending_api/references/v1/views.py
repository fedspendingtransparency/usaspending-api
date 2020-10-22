import hashlib
import json

from django.http import HttpResponseBadRequest
from rest_framework.response import Response
from rest_framework.views import APIView
from usaspending_api.references.models import FilterHash
from usaspending_api.references.v1.serializers import FilterSerializer, HashSerializer
from usaspending_api.common.api_versioning import deprecated
from django.utils.decorators import method_decorator


@method_decorator(deprecated, name="post")
class FilterEndpoint(APIView):
    """DEPRECATED"""

    serializer_class = FilterSerializer

    def post(self, request, format=None):
        """
        Return the hash for a received filters object
        """
        # get json
        # request.body is used because we want unicode as hash input
        json_req = request.body
        # create hash
        m = hashlib.md5()
        m.update(json_req)
        hash = m.hexdigest().encode("utf8")
        if len(str(hash)) > 2 and str(hash)[:2] == "b'":
            hash = str(hash)[2:-1]
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
        # return hash
        return Response({"hash": hash})


@method_decorator(deprecated, name="post")
class HashEndpoint(APIView):
    """DEPRECATED"""

    serializer_class = HashSerializer

    def post(self, request, format=None):
        """
        Return the stored filter object corresponding to the received hash
        """
        # get hash
        body_unicode = request.body.decode("utf-8")
        body = json.loads(body_unicode)
        hash = body["hash"]

        # check for hash in db
        try:
            fh = FilterHash.objects.get(hash=hash)
            # return filter json
            return Response({"filter": fh.filter})
        except FilterHash.DoesNotExist:
            return HttpResponseBadRequest(
                "The FilterHash Object with that has does not exist. DoesNotExist Error Thrown."
            )
