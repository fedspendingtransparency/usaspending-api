import json

from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response

from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.submissions.serializers import SubmissionAttributesSerializer
from usaspending_api.common.api_request_utils import (
    FilterGenerator, ResponsePaginator, DataQueryHandler)


class SubmissionAttributesList(APIView):

    def get(self, request):
        """Return a response for a submissin GET request."""

        subs = SubmissionAttributes.objects.all()

        fg = FilterGenerator()
        filter_arguments = fg.create_from_get(request.GET)

        subs = subs.filter(**filter_arguments)

        paged_data = ResponsePaginator.get_paged_data(subs, request_parameters=request.GET)

        serializer = SubmissionAttributesSerializer(paged_data, many=True)
        response_object = {
            "total_metadata": {
                "count": subs.count(),
            },
            "page_metadata": {
                "page_number": paged_data.number,
                "num_pages": paged_data.paginator.num_pages,
                "count": len(paged_data),
            },
            "results": serializer.data
        }
        return Response(response_object)

    def post(self, request):
        """Return a response for a submission POST request."""
        try:
            body_unicode = request.body.decode('utf-8')
            body = json.loads(body_unicode)
            dq = DataQueryHandler(
                SubmissionAttributes,
                SubmissionAttributesSerializer,
                body)
            response_data = dq.build_response()
        except Exception as e:
            return Response({"message": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        return Response(response_data)
