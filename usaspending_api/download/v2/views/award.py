from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.award import award_filter
from usaspending_api.download.v2.csv_creator import create_award_csv


class DownloadAwardsViewSet(APIView):

    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""

        json_request = self.get_request_payload(request)
        filters = json_request['filters']
        columns = json_request['columns']

        # filter Awards based on filter input
        queryset = award_filter(filters)
        result = create_award_csv(columns, queryset)

        # craft response
        response = {
            "total_size": result["total_size"],
            "total_columns": len(columns),
            "total_rows": result["total_rows"],
            "file_name": result["file_name"]
        }

        return Response(response)
