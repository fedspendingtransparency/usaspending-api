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
        # get agency's cgac code and use that code to get the agency's submission
        agency = Agency.objects.filter(id=agency_id).first()

        if agency is None:
            return Response(response)

        toptier_agency = agency.toptier_agency
        # get corresponding submissions through cgac code
        queryset = SubmissionAttributes.objects.all()
        queryset = queryset.filter(cgac_code=toptier_agency.cgac_code)

        # get the most up to date fy and quarter
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

        # get the overall total government budget authority (to craft a budget authority percentage)
        total_budget_authority_queryset = OverallTotals.objects.all()
        total_budget_authority_queryset = total_budget_authority_queryset.filter(fiscal_year=active_fiscal_year)

        total_budget_authority_submission = total_budget_authority_queryset.first()
        total_budget_authority_amount = ""

        if total_budget_authority_submission is not None:
            total_budget_authority_amount = str(total_budget_authority_submission.total_budget_authority)

        # craft response
        response['results'] = {'agency_name': toptier_agency.name,
                               'active_fy': str(active_fiscal_year),
                               'active_fq': str(active_fiscal_quarter),
                               'outlay_amount': str(submission.outlay_amount),
                               'obligated_amount': str(submission.obligated_amount),
                               'budget_authority_amount': str(submission.budget_authority_amount),
                               'total_budget_authority_amount': total_budget_authority_amount
                               }

        return Response(response)
