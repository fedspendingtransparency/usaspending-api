from rest_framework import generics
from rest_framework import mixins

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
        return self.aggregate(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        return self.aggregate(request, *args, **kwargs)
