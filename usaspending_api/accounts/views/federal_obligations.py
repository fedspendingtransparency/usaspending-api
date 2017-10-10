from django.db.models import F, Sum

from usaspending_api.accounts.serializers import FederalAccountByObligationSerializer
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.views import DetailViewSet
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.references.models import Agency


class FederalAccountByObligationViewSet(DetailViewSet):
    """Returns a Appropriation Account Balance's obligated amount broken up by TAS."""
    serializer_class = FederalAccountByObligationSerializer

    def get_queryset(self):
        """Returns FY total obligations by account in descending order."""
        # Retrieve post request payload
        json_request = self.request.query_params
        # Retrieve fiscal_year & agency_identifier from request
        fiscal_year = json_request.get('fiscal_year', None)
        funding_agency_id = json_request.get('funding_agency_id', None)
        # Raise exception if required query parameter not provided
        if not funding_agency_id:
            raise InvalidParameterException(
                'Missing required query parameters: fiscal_year & funding_agency_id'
            )
        # Use filter() instead of get() on Agency.objects
        # as get() will likely raise an error on a bad agency id
        # while trying to get the top_tier_agency_id from the Agency set
        top_tier_agency_id = Agency.objects.filter(id=funding_agency_id).first().toptier_agency_id
        # Using final_objects below ensures that we're only pulling the latest
        # set of financial information for each fiscal year
        queryset = AppropriationAccountBalances.final_objects.filter(
            submission__reporting_fiscal_year=fiscal_year,
            treasury_account_identifier__funding_toptier_agency=top_tier_agency_id
        ).annotate(
            agency_name=F('treasury_account_identifier__reporting_agency_name'),
            account_title=F('treasury_account_identifier__federal_account__account_title'),
            id=F('treasury_account_identifier__federal_account')

        )
        # Sum and sort descending obligations_incurred by account
        queryset = queryset.values(
            'id',
            'agency_name',
            'account_title'
        ).annotate(
            obligated_amount=Sum('obligations_incurred_total_by_tas_cpe')
        ).order_by('-obligated_amount')
        # Return minor object class vars
        return queryset
