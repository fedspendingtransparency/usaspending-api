from django.db.models import F, Sum

from usaspending_api.accounts.serializers import AgenciesFinancialBalancesSerializer
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.references.models import ToptierAgency
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.common.views import CachedDetailViewSet
from usaspending_api.common.exceptions import InvalidParameterException


class AgenciesFinancialBalancesViewSet(CachedDetailViewSet):
    """
    Returns financial balances by agency and the latest quarter for the given fiscal year.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/financial_balances/agencies.md"

    serializer_class = AgenciesFinancialBalancesSerializer

    def get_queryset(self, pk=None):
        # retrieve post request payload
        json_request = self.request.query_params
        # retrieve fiscal_year from request
        fiscal_year = json_request.get("fiscal_year", None)
        funding_agency_id = json_request.get("funding_agency_id", None)

        # required query parameters were not provided
        if not (fiscal_year and funding_agency_id):
            raise InvalidParameterException(
                "Missing one or more required query parameters: fiscal_year, funding_agency_id"
            )

        toptier_agency = ToptierAgency.objects.filter(agency__id=funding_agency_id).first()
        if toptier_agency is None:
            return AppropriationAccountBalances.objects.none()

        submission_queryset = SubmissionAttributes.objects.all()
        submission_queryset = (
            submission_queryset.filter(toptier_code=toptier_agency.toptier_code, reporting_fiscal_year=fiscal_year)
            .order_by("-reporting_fiscal_year", "-reporting_fiscal_quarter")
            .annotate(fiscal_year=F("reporting_fiscal_year"), fiscal_quarter=F("reporting_fiscal_quarter"))
        )
        submission = submission_queryset.first()

        if submission is None:
            return AppropriationAccountBalances.objects.none()
        active_fiscal_year = submission.reporting_fiscal_year
        active_fiscal_quarter = submission.fiscal_quarter

        queryset = (
            AppropriationAccountBalances.objects.filter(
                submission__reporting_fiscal_year=active_fiscal_year,
                submission__reporting_fiscal_quarter=active_fiscal_quarter,
                treasury_account_identifier__funding_toptier_agency=toptier_agency,
                submission__is_final_balances_for_fy=True,
            )
            .annotate(fiscal_year=F("submission__reporting_fiscal_year"))
            .values("fiscal_year")
            .annotate(
                budget_authority_amount=Sum("total_budgetary_resources_amount_cpe"),
                obligated_amount=Sum("obligations_incurred_total_by_tas_cpe"),
                outlay_amount=Sum("gross_outlay_amount_by_tas_cpe"),
            )
        )

        return queryset
