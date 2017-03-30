from usaspending_api.accounts.serializers import FederalAccountSerializer
from usaspending_api.accounts.models import FederalAccount
from usaspending_api.common.mixins import FilterQuerysetMixin, ResponseMetadatasetMixin
from usaspending_api.common.views import DetailViewSet, AutocompleteView
from usaspending_api.common.mixins import SuperLoggingMixin


class FederalAccountViewSet(SuperLoggingMixin,
                            FilterQuerysetMixin,
                            ResponseMetadatasetMixin,
                            DetailViewSet):
    """Handle requests for federal account information."""
    serializer_class = FederalAccountSerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = FederalAccount.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class FederalAccountAutocomplete(FilterQuerysetMixin,
                                 AutocompleteView):
    """Handle autocomplete requests for federal account information."""
    serializer_class = FederalAccountSerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = FederalAccount.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        return filtered_queryset
