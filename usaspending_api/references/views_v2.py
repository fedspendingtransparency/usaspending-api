from django.db.models import F, Sum
from usaspending_api.references.models import Agency
from usaspending_api.submissions.models import SubmissionAttributes
from rest_framework.views import APIView
from rest_framework.response import Response


class AgencyEndpoint(APIView):
    """Return an agency name and active fy"""
    def get(self, request, pk, format=None):
        """Return the view's queryset."""
        response = {'results': {}}

        # get id from url
        agency_id = int(pk)
        # get agency cgac code
        agency = Agency.objects.filter(id=agency_id).first()

        if agency is None:
            return Response(response)

        toptier_agency = agency.toptier_agency
        # get corresponding submissions through cgac code
        queryset = SubmissionAttributes.objects.all()
        queryset = queryset.filter(cgac_code=toptier_agency.cgac_code)

        # get the most up to date fy
        queryset = queryset.order_by('-reporting_fiscal_year')
        queryset = queryset.annotate(
            fiscal_year=F('reporting_fiscal_year')
        )
        submission = queryset.first()
        if submission is None:
            return Response(response)
        active_fiscal_year = submission.fiscal_year

        # craft response
        response['results']['agency_name'] = toptier_agency.name
        response['results']['active_fiscal_year'] = str(active_fiscal_year)
        return Response(response)
