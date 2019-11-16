from usaspending_api.accounts.serializers import FederalAccountSerializer
from usaspending_api.accounts.models import FederalAccount
from usaspending_api.common.mixins import FilterQuerysetMixin
from usaspending_api.common.views import CachedDetailViewSet, AutocompleteView
from usaspending_api.common.api_versioning import deprecated, removed
from django.utils.decorators import method_decorator


@method_decorator(removed, name="post")
class FederalAccountAutocomplete(FilterQuerysetMixin, AutocompleteView):
    """
    Handle autocomplete requests for federal account information.
    """

    serializer_class = FederalAccountSerializer

    def get_queryset(self):
        """
        Return the view's queryset.
        """
        queryset = FederalAccount.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


@method_decorator(deprecated, name="list")
@method_decorator(deprecated, name="retrieve")
class FederalAccountViewSet(FilterQuerysetMixin, CachedDetailViewSet):
    """
    Handle requests for federal account information.
    """

    serializer_class = FederalAccountSerializer

    def get_queryset(self):
        """
        Return the view's queryset.
        """
        queryset = FederalAccount.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset
