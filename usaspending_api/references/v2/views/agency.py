from django.db.models import F, Sum
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.date_helper import now
from usaspending_api.references.models import Agency, GTASSF133Balances
from usaspending_api.submissions.models import SubmissionAttributes


class AgencyViewSet(APIView):
    """
    Return an agency name and active fy.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/agency/id.md"

    @cache_response()
    def get(self, request, pk, format=None):
        """
        Return the view's queryset.
        """
        response = {"results": {}}
        # get id from url
        agency_id = int(pk)
        # get agency's cgac code and use that code to get the agency's submission
        agency = Agency.objects.filter(id=agency_id).first()

        if agency is None:
            return Response(response)

        toptier_agency = agency.toptier_agency
        # get corresponding submissions through cgac code
        queryset = SubmissionAttributes.objects.all()
        queryset = queryset.filter(
            toptier_code=toptier_agency.toptier_code, submission_window__submission_reveal_date__lte=now()
        )

        # get the most up to date fy, quarter, and period
        queryset = queryset.order_by("-reporting_fiscal_year", "-reporting_fiscal_quarter", "-reporting_fiscal_period")
        queryset = queryset.annotate(
            fiscal_year=F("reporting_fiscal_year"), fiscal_quarter=F("reporting_fiscal_quarter")
        )
        submission = queryset.first()
        if submission is None:
            return Response(response)
        active_fiscal_year = submission.reporting_fiscal_year
        active_fiscal_quarter = submission.fiscal_quarter
        active_fiscal_period = submission.reporting_fiscal_period

        queryset = AppropriationAccountBalances.objects.filter(submission__is_final_balances_for_fy=True)
        # get the incoming agency's toptier agency, because that's what we'll
        # need to filter on
        # (used filter() instead of get() b/c we likely don't want to raise an
        # error on a bad agency id)
        aggregate_dict = queryset.filter(
            submission__reporting_fiscal_year=active_fiscal_year,
            submission__reporting_fiscal_quarter=active_fiscal_quarter,
            treasury_account_identifier__funding_toptier_agency=toptier_agency,
        ).aggregate(
            budget_authority_amount=Sum("total_budgetary_resources_amount_cpe"),
            obligated_amount=Sum("obligations_incurred_total_by_tas_cpe"),
            outlay_amount=Sum("gross_outlay_amount_by_tas_cpe"),
        )

        cj = toptier_agency.justification if toptier_agency.justification else None

        total_budgetary_resources = (
            GTASSF133Balances.objects.filter(fiscal_year=active_fiscal_year, fiscal_period=active_fiscal_period)
            .values("fiscal_year")
            .aggregate(Sum("total_budgetary_resources_cpe"))
        )
        total_budgetary_resources = total_budgetary_resources.get("total_budgetary_resources_cpe__sum", 0)

        # craft response
        response["results"] = {
            "agency_name": toptier_agency.name,
            "active_fy": str(active_fiscal_year),
            "active_fq": str(active_fiscal_quarter),
            "outlay_amount": str(aggregate_dict["outlay_amount"]),
            "obligated_amount": str(aggregate_dict["obligated_amount"]),
            "budget_authority_amount": str(aggregate_dict["budget_authority_amount"]),
            "current_total_budget_authority_amount": str(total_budgetary_resources),
            "mission": toptier_agency.mission,
            "website": toptier_agency.website,
            "icon_filename": toptier_agency.icon_filename,
            "congressional_justification_url": cj,
        }

        return Response(response)
