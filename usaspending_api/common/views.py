from rest_framework import generics
from rest_framework import mixins
from rest_framework.response import Response

from usaspending_api.common.api_request_utils import ResponsePaginator
from usaspending_api.common.serializers import AggregateSerializer
from usaspending_api.common.mixins import AggregateQuerysetMixin


class AggregateView(mixins.ListModelMixin,
                    AggregateQuerysetMixin,
                    generics.GenericAPIView):
    """
    Handles the view for endpoints that request aggregated data.
    The endpoint views inherit from this custom view instead of
    using GenericAPIView directly because we need to handle POST
    requests in addition to GET requests. We may want to re-think
    this in the future.
    """
    serializer_class = AggregateSerializer

    def get(self, request, *args, **kwargs):
        queryset = self.aggregate(request, *args, **kwargs)
        return Response(serializer.data)

    def post(self, request, *args, **kwargs):
        queryset = self.aggregate(request, *args, **kwargs)

        # construct metadata of entire queryset
        metadata = {"count": queryset.count()}

        # get paged data for this request
        paged_data = ResponsePaginator.get_paged_data(
            queryset, request_parameters=request.data)
        paged_queryset = paged_data.object_list.all()

        # construct page-specific metadata
        page_metadata = {
            "page_number": paged_data.number,
            "num_pages": paged_data.paginator.num_pages,
            "count": len(paged_data)
        }

        # serialize the paged data
        serializer = self.get_serializer(paged_queryset, many=True)
        serialized_data = serializer.data

        response_object = {
            "total_metadata": metadata,
            "page_metadata": page_metadata
        }
        response_object.update({'results': serialized_data})
        return Response(response_object)

