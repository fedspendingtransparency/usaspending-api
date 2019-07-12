from django.db.models import F, Sum

from usaspending_api.accounts.serializers import FederalAccountByObligationSerializer
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.views import CachedDetailViewSet
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.references.models import Agency
from usaspending_api.references.constants import DOD_ARMED_FORCES_CGAC, DOD_CGAC


class FederalAccountByObligationViewSet(CachedDetailViewSet):
    """
    Returns a Appropriation Account Balance's obligated amount broken up by TAS.
    endpoint_doc: /federal_obligations.md
    """

    serializer_class = FederalAccountByObligationSerializer

    def get_queryset(self):
        """
        Returns FY total obligations by account in descending order.
        """
        # Retrieve post request payload
        json_request = self.request.query_params
        # Retrieve fiscal_year & agency_identifier from request
        fiscal_year = json_request.get("fiscal_year", None)
        funding_agency_id = json_request.get("funding_agency_id", None)
        # Raise exception if required query parameter not provided
        if not funding_agency_id:
            raise InvalidParameterException("Missing required query parameters: fiscal_year & funding_agency_id")
        # Use filter() instead of get() on Agency.objects
        # as get() will likely raise an error on a bad agency id
        # while trying to get the top_tier_agency_id from the Agency set
        toptier_agency = Agency.objects.filter(id=funding_agency_id).first().toptier_agency
        # Using final_objects below ensures that we're only pulling the latest
        # set of financial information for each fiscal year
        # DS-1655: if the AID is "097" (DOD), Include the branches of the military in the queryset
        if toptier_agency.cgac_code == DOD_CGAC:
            tta_list = DOD_ARMED_FORCES_CGAC
            queryset = AppropriationAccountBalances.final_objects.filter(
                submission__reporting_fiscal_year=fiscal_year,
                treasury_account_identifier__funding_toptier_agency__cgac_code__in=tta_list,
            )
        else:
            queryset = AppropriationAccountBalances.final_objects.filter(
                submission__reporting_fiscal_year=fiscal_year,
                treasury_account_identifier__funding_toptier_agency__cgac_code=toptier_agency.cgac_code,
            )

        queryset = queryset.annotate(
            account_title=F("treasury_account_identifier__federal_account__account_title"),
            id=F("treasury_account_identifier__federal_account"),
        )
        # Sum and sort descending obligations_incurred by account
        queryset = (
            queryset.values("id", "account_title")
            .annotate(
                account_number=F("treasury_account_identifier__federal_account__federal_account_code"),
                obligated_amount=Sum("obligations_incurred_total_by_tas_cpe"),
            )
            .order_by("-obligated_amount")
        )
        # Return minor object class vars
        return queryset
