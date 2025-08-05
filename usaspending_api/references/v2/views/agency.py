from django.db.models import DecimalField, Q, Sum
from django.db.models.functions import Coalesce
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.date_helper import now
from usaspending_api.references.models import Agency, GTASSF133Balances
from usaspending_api.submissions.helpers import get_last_closed_submission_date
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

        # Get corresponding submissions through cgac code with Submission in latest Quarter / Period
        submission_queryset = SubmissionAttributes.objects.filter(
            toptier_code=toptier_agency.toptier_code,
            submission_window__submission_reveal_date__lte=now(),
        )

        if not submission_queryset.exists():
            return Response(response)

        most_recent_quarter_window_id = get_last_closed_submission_date(is_quarter=True)["id"]
        most_recent_period_window_id = get_last_closed_submission_date(is_quarter=False)["id"]

        submission = (
            submission_queryset.filter(
                Q(submission_window=most_recent_quarter_window_id) | Q(submission_window=most_recent_period_window_id)
            )
            .order_by("-reporting_fiscal_year", "-reporting_fiscal_quarter", "-reporting_fiscal_period")
            .first()
        )
        if submission is None:
            # If the given agency has no submission in the latest FP or FQ, grab the latest seen submission
            # (from any arbitrary agency) to derive the "active" (latest closed) FY, FQ, and FP.
            # Not terminating early as we do above since the Agency does have at least one historical submission.
            submission = (
                SubmissionAttributes.objects.filter(submission_window__submission_reveal_date__lte=now())
                .order_by("-reporting_fiscal_year", "-reporting_fiscal_quarter", "-reporting_fiscal_period")
                .first()
            )

        active_fiscal_year = submission.reporting_fiscal_year
        active_fiscal_quarter = submission.reporting_fiscal_quarter
        active_fiscal_period = submission.reporting_fiscal_period

        # If an Agency does not have a Submission in the recent FY, FQ, and FP combination then this query will be
        # returning 0s for all sums since it does an INNER JOIN to the Submission pulled by the queries above.
        queryset = AppropriationAccountBalances.objects.filter(submission__is_final_balances_for_fy=True)

        aggregate_dict = queryset.filter(
            submission__reporting_fiscal_year=active_fiscal_year,
            submission__reporting_fiscal_quarter=active_fiscal_quarter,
            treasury_account_identifier__funding_toptier_agency=toptier_agency,
        ).aggregate(
            budget_authority_amount=Coalesce(
                Sum("total_budgetary_resources_amount_cpe"),
                0,
                output_field=DecimalField(max_digits=23, decimal_places=2),
            ),
            obligated_amount=Coalesce(
                Sum("obligations_incurred_total_by_tas_cpe"),
                0,
                output_field=DecimalField(max_digits=23, decimal_places=2),
            ),
            outlay_amount=Coalesce(
                Sum("gross_outlay_amount_by_tas_cpe"), 0, output_field=DecimalField(max_digits=23, decimal_places=2)
            ),
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
