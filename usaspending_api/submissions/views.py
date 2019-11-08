from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.submissions.serializers import SubmissionAttributesSerializer
from usaspending_api.common.mixins import FilterQuerysetMixin
from usaspending_api.common.views import CachedDetailViewSet
from usaspending_api.common.api_versioning import removed
from django.utils.decorators import method_decorator


@method_decorator(removed, name="list")
class SubmissionAttributesViewSet(FilterQuerysetMixin, CachedDetailViewSet):
    """
    Handles requests for information about data submissions.
    """

    serializer_class = SubmissionAttributesSerializer

    def get_queryset(self):
        """
        Return the view's queryset.
        """
        queryset = SubmissionAttributes.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset
