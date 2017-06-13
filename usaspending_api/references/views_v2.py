from django.db.models import F, Sum
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
        # get args
        agency_id = json_request.get('agency_id', None)
        #
        queryset = AppropriationAccountBalances.final_objects.all()
        toptier_agency = Agency.objects.filter(id=agency_id).first().toptier_agency

        queryset = queryset.filter(
            #submission__reporting_fiscal_year=fiscal_year,
            treasury_account_identifier__funding_toptier_agency=toptier_agency
        )



        queryset = queryset.values('agency_name').annotate(
            fiscal_year=F('submission__reporting_fiscal_year'))
        # queryset = queryset.values('agency_name','fiscal_year').annotate(
        #    fiscal_year=Sum('budget_authority_available_amount_total_cpe'))


        return queryset
