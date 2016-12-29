from rest_framework import viewsets
from rest_framework.response import Response

from usaspending_api.common.api_request_utils import ResponsePaginator
from usaspending_api.common.serializers import AggregateSerializer
from usaspending_api.common.mixins import AggregateQuerysetMixin


class AggregateView(AggregateQuerysetMixin,
                    viewsets.ReadOnlyModelViewSet):
    """
    Handles the view for endpoints that request aggregated data.
    The endpoint views inherit from this custom view instead of
    using GenericAPIView directly because we need to handle POST
    requests in addition to GET requests. We may want to re-think
    this in the future.
    """
    serializer_class = AggregateSerializer

    def list(self, request, *args, **kwargs):
        """
        Override the parent list method so we can aggregate the data
        before constructing a respones.
        """
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


class DetailViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Handles the views for endpoints that request a detailed
    view of model objects (either in the form of a single
    object or a list of objects).

    Note that the only reason our views don't use Django
    Rest Frameworks ReadOnlyModelViewSet directly is that
    we're overriding the list method to tack metadata
    on to our responses. Once we're able to customize
    our metadata via DRF pagination, we probably won't
    need this class at all and can just hook views directly
    to viewsets.ReadOnlyModelViewSet.
    """

    def list(self, request, *args, **kwargs):
        """
        Override the parent list method so we can add metadata to response.
        Once we're able to customize metadata via DRF pagination, we won't need this.
        """
        response = self.build_response(
            self.request, queryset=self.get_queryset(), serializer=self.get_serializer_class())
        return Response(response)
