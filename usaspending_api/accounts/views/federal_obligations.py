from django.db.models import F, Sum
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.accounts.serializers import FederalAccountByObligationSerializer
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.views import CachedDetailViewSet


class FederalAccountByObligationViewSet(CachedDetailViewSet):
    """
    Returns a Appropriation Account Balance's obligated amount broken up by TAS.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/federal_obligations.md"

    serializer_class = FederalAccountByObligationSerializer

    def get_queryset(self):
        """
        Returns FY total obligations by account in descending order.
        """
        # Retrieve post request payload
        json_request = self.request.query_params

        # Retrieve fiscal_year & agency_identifier from request
        fiscal_year = json_request.get("fiscal_year")
        funding_agency_id = json_request.get("funding_agency_id")

        # Raise exception if required query parameter not provided
        if not funding_agency_id or not fiscal_year:
            raise InvalidParameterException("Missing required query parameters: fiscal_year & funding_agency_id")

        queryset = (
            AppropriationAccountBalances.objects.filter(
                treasury_account_identifier__funding_toptier_agency__agency__id=funding_agency_id,
                submission__reporting_fiscal_year=fiscal_year,
                submission__is_final_balances_for_fy=True,
            )
            .annotate(
                account_title=F("treasury_account_identifier__federal_account__account_title"),
                id=F("treasury_account_identifier__federal_account"),
            )
            .values("id", "account_title")
            .annotate(
                account_number=F("treasury_account_identifier__federal_account__federal_account_code"),
                obligated_amount=Sum("obligations_incurred_total_by_tas_cpe"),
            )
            .order_by("-obligated_amount", "treasury_account_identifier__federal_account__federal_account_code")
        )

        return queryset
