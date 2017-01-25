from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.submissions.serializers import SubmissionAttributesSerializer
from usaspending_api.common.mixins import FilterQuerysetMixin, ResponseMetadatasetMixin
from usaspending_api.common.views import DetailViewSet


class SubmissionAttributesViewSet(FilterQuerysetMixin,
                                  ResponseMetadatasetMixin,
                                  DetailViewSet):
    """Handles requests for information about data submissions."""

    serializer_class = SubmissionAttributesSerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = SubmissionAttributes.objects.all()
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset
