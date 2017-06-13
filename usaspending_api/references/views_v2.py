from django.db.models import F, Sum
from usaspending_api.references.models import Agency
from usaspending_api.common.mixins import FilterQuerysetMixin, SuperLoggingMixin
from usaspending_api.common.views import DetailViewSet, AutocompleteView
from usaspending_api.references.serializers_v2 import AgencyV2Serializer
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.submissions.models import SubmissionAttributes
from rest_framework.views import APIView
from rest_framework.response import Response


class AgencyEndpoint(APIView):
    """Return an agency"""
    #serializer_class = AgencyV2Serializer

    def get(self, request, pk, format=None):
        """Return the view's queryset."""
        # get id
        agency_id = int(pk) #self.kwargs['pk']
        #print(pk)
        # get most up to date fiscal year
        #queryset = AppropriationAccountBalances.final_objects.all()
        #toptier_agency = Agency.objects.filter(id=agency_id).first().toptier_agency
        # queryset = queryset.filter(
        #     treasury_account_identifier__funding_toptier_agency=toptier_agency
        # )

        queryset = SubmissionAttributes.objects.all()
        toptier_agency = Agency.objects.filter(id=agency_id).first().toptier_agency
        queryset = queryset.filter(cgac_code=toptier_agency.cgac_code)

        # get the most up to date fy
        queryset = queryset.order_by('-reporting_fiscal_year')
        queryset = queryset.annotate(
            fiscal_year=F('reporting_fiscal_year')
        )
        active_fiscal_year = queryset.first().fiscal_year

        # craft response
        agency_name = toptier_agency.name
        active_fiscal_year = active_fiscal_year
        return Response({'agency_name': agency_name, 'active_fiscal_year': str(active_fiscal_year)})
