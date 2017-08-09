from django.contrib.postgres.search import TrigramSimilarity, SearchVector
from django.db.models import F
from django.db.models.functions import Greatest
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.models import Award
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import Agency, Cfda
from usaspending_api.references.v1.serializers import AgencySerializer


class DownloadAwardViewSet(APIView):

    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""

        json_request = self.get_request_payload(request)
        filters = json_request['filters']
        columns = json_request['columns']

        # filter Awards based on filter input
        queryset = Award.objects.all()
        


        # create columns

        # craft response
        response = {
            "total_size": 0,
            "total_columns": len(columns),
            "total_rows": 0,
            "file_name": "test"
        }

        return Response(response)


