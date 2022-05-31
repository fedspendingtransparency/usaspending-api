from django.db.models import DecimalField, F, Max, OuterRef, Q, Sum, Subquery
from django.db.models.functions import Coalesce
from django.utils.text import slugify
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.date_helper import now
from usaspending_api.common.helpers.generic_helper import sort_with_null_last
from usaspending_api.references.models import Agency, GTASSF133Balances
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.submissions.helpers import get_last_closed_submission_date
from usaspending_api.submissions.models import SubmissionAttributes


class ToptierAgenciesViewSet(APIView):
    """
    This route sends a request to the backend to retrieve all toptier agencies and related, relevant data.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/toptier_agencies.md"

    @cache_response()
    def get(self, request, format=None):
        sortable_columns = [
            "agency_id",
            "agency_name",
            "active_fy",
            "active_fq",
            "outlay_amount",
            "obligated_amount",
            "budget_authority_amount",
            "current_total_budget_authority_amount",
            "percentage_of_total_budget_authority",
        ]

        sort = request.query_params.get("sort", "agency_name")
        order = request.query_params.get("order", "asc")
        response = {"results": []}

        if sort not in sortable_columns:
            raise InvalidParameterException(
                "The sort value provided is not a valid option. "
                "Please choose from the following: " + str(sortable_columns)
            )

        if order not in ["asc", "desc"]:
            raise InvalidParameterException(
                "The order value provided is not a valid option. Please choose from the following: ['asc', 'desc']"
            )

        # Subquery does not generate a Group By for the Queryset so "trigger_group_by" is added;
        tbr_by_year_and_period = (
            GTASSF133Balances.objects.values("fiscal_year", "fiscal_period")
            .annotate(
                trigger_group_by=Max("fiscal_period"),
            )
            .annotate(
                total_budgetary_resources=Subquery(
                    GTASSF133Balances.objects.filter(
                        fiscal_year=OuterRef("fiscal_year"), fiscal_period=OuterRef("fiscal_period")
                    )
                    .values("fiscal_year")
                    .annotate(total_budgetary_resources=Sum("total_budgetary_resources_cpe"))
                    .values("total_budgetary_resources")
                ),
            )
            .values("fiscal_year", "fiscal_period", "total_budgetary_resources")
        )
        tbr_by_year_and_period = {
            (val["fiscal_year"], val["fiscal_period"]): val["total_budgetary_resources"]
            for val in tbr_by_year_and_period
        }

        # Get list of Agencies that submitted in the most recent Quarter / Period submission window
        most_recent_quarter_window = get_last_closed_submission_date(is_quarter=True)
        most_recent_period_window = get_last_closed_submission_date(is_quarter=False)

        all_toptier_with_submission = (
            SubmissionAttributes.objects.filter(submission_window__submission_reveal_date__lte=now())
            .distinct("toptier_code")
            .values_list("toptier_code", flat=True)
        )
        latest_submission_by_toptier = (
            SubmissionAttributes.objects.filter(
                Q(submission_window=most_recent_quarter_window["id"])
                | Q(submission_window=most_recent_period_window["id"])
            )
            .order_by("toptier_code", "-reporting_fiscal_year", "-reporting_fiscal_quarter", "-reporting_fiscal_period")
            .distinct("toptier_code")
            .values("toptier_code", "reporting_fiscal_year", "reporting_fiscal_quarter", "reporting_fiscal_period")
        )
        latest_submission_by_toptier = {
            val["toptier_code"]: {
                "fiscal_year": val["reporting_fiscal_year"],
                "fiscal_quarter": val["reporting_fiscal_quarter"],
                "fiscal_period": val["reporting_fiscal_period"],
            }
            for val in latest_submission_by_toptier
        }

        placeholder = {
            "fiscal_year": most_recent_period_window["submission_fiscal_year"],
            "fiscal_quarter": most_recent_period_window["submission_fiscal_quarter"],
            "fiscal_period": most_recent_period_window["submission_fiscal_month"],
        }
        for toptier_code in all_toptier_with_submission:
            submission_values = latest_submission_by_toptier.get(toptier_code)

            # Add a placeholder for Agencies that have submitted but not in recent Quarter / Period;
            # Placeholder is most recent Period window to ensure accurate "current_total_budget_authority_amount"
            if submission_values is None:
                latest_submission_by_toptier[toptier_code] = placeholder

        aab_sums_by_toptier = (
            AppropriationAccountBalances.objects.filter(submission__is_final_balances_for_fy=True)
            .values(
                "treasury_account_identifier__funding_toptier_agency",
                "submission__reporting_fiscal_year",
                "submission__reporting_fiscal_quarter",
            )
            .annotate(
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
        )
        aab_sums_by_toptier = {
            (
                val["treasury_account_identifier__funding_toptier_agency"],
                val["submission__reporting_fiscal_year"],
                val["submission__reporting_fiscal_quarter"],
            ): {
                "budget_authority_amount": val["budget_authority_amount"],
                "obligated_amount": val["obligated_amount"],
                "outlay_amount": val["outlay_amount"],
            }
            for val in aab_sums_by_toptier
        }

        # get agency queryset, distinct toptier id to avoid duplicates, take first ordered agency id for consistency
        agency_list = (
            Agency.objects.order_by("toptier_agency_id", "id")
            .distinct("toptier_agency_id")
            .values()
            .annotate(
                justification=F("toptier_agency__justification"),
                toptier_abbreviation=F("toptier_agency__abbreviation"),
                toptier_name=F("toptier_agency__name"),
                toptier_code=F("toptier_agency__toptier_code"),
            )
        )

        for agency in agency_list:
            submission = latest_submission_by_toptier.get(agency["toptier_code"])

            # This means we don't have any submissions for that Agency and therefore should not be showing them
            if submission is None:
                continue

            active_fiscal_year = submission["fiscal_year"]
            active_fiscal_quarter = submission["fiscal_quarter"]
            active_fiscal_period = submission["fiscal_period"]

            default_aab_sums = {"outlay_amount": 0, "obligated_amount": 0, "budget_authority_amount": 0}
            aab_sums = aab_sums_by_toptier.get(
                (agency["toptier_agency_id"], active_fiscal_year, active_fiscal_quarter), default_aab_sums
            )

            abbreviation = agency.get("toptier_abbreviation", "")
            cj = agency.get("justification")
            total_budgetary_resources = tbr_by_year_and_period.get((active_fiscal_year, active_fiscal_period), 0)

            # craft response
            response["results"].append(
                {
                    "agency_id": agency["id"],
                    "toptier_code": agency["toptier_code"],
                    "abbreviation": abbreviation,
                    "agency_name": agency["toptier_name"],
                    "congressional_justification_url": cj,
                    "active_fy": str(active_fiscal_year),
                    "active_fq": str(active_fiscal_quarter),
                    "outlay_amount": float(aab_sums["outlay_amount"]),
                    "obligated_amount": float(aab_sums["obligated_amount"]),
                    "budget_authority_amount": float(aab_sums["budget_authority_amount"]),
                    "current_total_budget_authority_amount": float(total_budgetary_resources),
                    "percentage_of_total_budget_authority": (
                        (float(aab_sums["budget_authority_amount"]) / float(total_budgetary_resources))
                        if total_budgetary_resources > 0
                        else None
                    ),
                    "agency_slug": slugify(agency["toptier_name"]),
                }
            )

        response["results"] = sort_with_null_last(
            to_sort=response["results"], sort_key=sort, sort_order=order, tie_breaker="agency_name"
        )

        return Response(response)
