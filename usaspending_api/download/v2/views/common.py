from django.contrib.postgres.search import TrigramSimilarity, SearchVector
from django.db.models import F
from django.db.models.functions import Greatest
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.models import LegalEntity, TreasuryAppropriationAccount, TransactionContract
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import Agency, Cfda
from usaspending_api.references.v1.serializers import AgencySerializer


class DownloadStatusViewSet(APIView):

    def post(self, request):
        """Return status of a csv"""

        json_request = self.get_request_payload(request)

        file_name = json_request["file_name"]
        type = json_request["type"]

        # Filter based on search text
        response = {}

        response['status'] = "test"

        response['url'] = "test"

        response['message'] = "test"

        return Response(response)


class DownloadColumnsViewSet(APIView):

    def get(self, request, pk, format=None):
        """Get columns based on download type"""
        response = {'columns': {}}
        if pk == "award":
            response['columns'] = {

            }
        elif pk == "transaction":
            response['columns'] = {

            }

        return Response(response)
