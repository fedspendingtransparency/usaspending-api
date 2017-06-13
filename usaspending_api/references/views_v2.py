from django.db.models import F
from usaspending_api.references.models import Agency
from usaspending_api.common.mixins import FilterQuerysetMixin, SuperLoggingMixin
from usaspending_api.common.views import DetailViewSet, AutocompleteView
from usaspending_api.references.serializers_v2 import AgencyV2Serializer
from usaspending_api.accounts.models import AppropriationAccountBalances



class AgencyEndpoint(SuperLoggingMixin,
                     FilterQuerysetMixin,
                     DetailViewSet):
    """Return an agency"""
    serializer_class = AgencyV2Serializer

    def get_queryset(self):
        """Return the view's queryset."""
        json_request = self.request.query_params
        agency_id = json_request.get('agency_id', None)

        queryset = Agency.objects.filter(id=agency_id)

        queryset = queryset.annotate(
            agency_name=F('top_tier_agency__name'))
        queryset = queryset.values('fiscal_year').annotate(
            fiscal_year=Sum('budget_authority_available_amount_total_cpe'))

        queryset = AppropriationAccountBalances.final_objects.all()
        # get the incoming agency's toptier agency, because that's what we'll
        # need to filter on
        # (used filter() instead of get() b/c we likely don't want to raise an
        # error on a bad agency id)
        toptier_agency = Agency.objects.filter(id=agency_id).first().toptier_agency
        queryset = queryset.filter(
            submission__reporting_fiscal_year=fiscal_year,
            treasury_account_identifier__funding_toptier_agency=toptier_agency
        )

        queryset = queryset.annotate(
            fiscal_year=F('submission__reporting_fiscal_year'))
        return queryset
