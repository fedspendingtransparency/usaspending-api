from django.db.models import F, Sum
from usaspending_api.references.models import Agency, OverallTotals
from usaspending_api.submissions.models import SubmissionAttributes
from rest_framework.views import APIView
from rest_framework.response import Response
from usaspending_api.accounts.models import AppropriationAccountBalances


class AgencyViewSet(APIView):
    """Return an agency name and active fy"""
    def get(self, request, pk, format=None):
        """Return the view's queryset."""
        response = {'results': {}}
        # get id from url
        agency_id = int(pk)
        # get agency cgac code
        agency = Agency.objects.filter(id=agency_id).first()

        if agency is None:
            print("agency does not exist")
            return Response(response)

        toptier_agency = agency.toptier_agency
        # get corresponding submissions through cgac code
        queryset = SubmissionAttributes.objects.all()
        queryset = queryset.filter(cgac_code=toptier_agency.cgac_code)

        # get the most up to date fy
        queryset = queryset.order_by('-reporting_fiscal_year', '-reporting_fiscal_quarter')
        queryset = queryset.annotate(
            fiscal_year=F('reporting_fiscal_year'),
            fiscal_quarter=F('reporting_fiscal_quarter')
        )
        submission = queryset.first()
        if submission is None:
            return Response(response)
        active_fiscal_year = submission.reporting_fiscal_year
        active_fiscal_quarter = submission.fiscal_quarter

        # craft response
        # response['results']['agency_name'] = toptier_agency.name
        # response['results']['active_fy'] = str(active_fiscal_year)
        # response['results']['active_fq'] = str(active_fiscal_quarter)

        # using final_objects below ensures that we're only pulling the latest
        # set of financial information for each fiscal year
        queryset = AppropriationAccountBalances.final_objects.all()
        # get the incoming agency's toptier agency, because that's what we'll
        # need to filter on
        # (used filter() instead of get() b/c we likely don't want to raise an
        # error on a bad agency id)
        queryset = queryset.filter(
            submission__reporting_fiscal_year=active_fiscal_year,
            treasury_account_identifier__funding_toptier_agency=toptier_agency
        )
        queryset = queryset.annotate(
            budget_authority_amount=Sum('budget_authority_available_amount_total_cpe'),
            obligated_amount=Sum('obligations_incurred_total_by_tas_cpe'),
            outlay_amount=Sum('gross_outlay_amount_by_tas_cpe'))

        submission = queryset.first()
        if submission is None:
            return Response(response)

        # craft more of the response
        # response['results']['budget_authority_amount'] = str(submission.budget_authority_amount)
        # response['results']['obligated_amount'] = str(submission.obligated_amount)
        # response['results']['outlay_amount'] = str(submission.outlay_amount)

        queryset2 = OverallTotals.objects.all()
        queryset2 = queryset2.filter(fiscal_year=active_fiscal_year)

        submission2 = queryset2.first()
        # response['results']['total_budget_authority_amount'] = str(submission.total_budget_authority)

        #craft response
        response['results']['total_budget_authority_amount'] = str(submission2.total_budget_authority)

        response['results']['budget_authority_amount'] = str(submission.budget_authority_amount)
        response['results']['obligated_amount'] = str(submission.obligated_amount)
        response['results']['outlay_amount'] = str(submission.outlay_amount)

        response['results']['active_fq'] = str(active_fiscal_quarter)
        response['results']['active_fy'] = str(active_fiscal_year)
        response['results']['agency_name'] = toptier_agency.name

        return Response(response)
