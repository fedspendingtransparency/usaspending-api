from django.db.models import F, Sum

from usaspending_api.accounts.serializers import FederalAccountByObligationSerializer
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.views import DetailViewSet
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import Agency


class FederalAccountByObligationViewSet(DetailViewSet):
    """Handle autocomplete requests for federal account information."""
    serializer_class = FederalAccountByObligationSerializer

    def get_queryset(self):
        """

        """
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
        # get() will likely raise an error on a bad agency id
        # Get the top_tier_agency_id from the Agency set
        top_tier_agency_id = Agency.objects.filter(id=funding_agency_id).first().toptier_agency_id
        print(top_tier_agency_id)

        # Using final_objects below ensures that we're only pulling the latest
        # set of financial information for each fiscal year
        queryset = FinancialAccountsByProgramActivityObjectClass.final_objects.filter(
            submission__reporting_fiscal_year=fiscal_year,
            treasury_account__funding_toptier_agency=top_tier_agency_id
        ).annotate(
            agency_name=F('treasury_account__reporting_agency_name'),
            account_title=F('treasury_account__account_title')
        )

        # Sum and sort descending obligated_amount by account
        queryset = queryset.values('agency_name', 'account_title').annotate(
            obligated_amount=Sum('obligations_incurred_by_program_object_class_cpe')).order_by('-obligated_amount')

        # Return minor object class vars
        return queryset

    """
    def get_queryset(self):
        
        # Retrieve post request payload
        json_request = self.request.query_params

        # Retrieve fiscal_year & agency_identifier from request
        agency_identifier = json_request.get('agency_identifier', None)

        # Raise exception if required query parameter not provided
        if not agency_identifier:
            raise InvalidParameterException(
                'Missing required query parameters: agency_identifier'
            )

        treasury_set = TreasuryAppropriationAccount.objects.filter(
            agency_id=agency_identifier).order_by('account_title')

        # The query is executed and cached
        for account in treasury_set:
            account_title = account.account_title
            #print(account_title)

        federal_account_set = FederalAccount.objects.filter(
            agency_identifier=agency_identifier).order_by('account_title')

        # The query is executed and cached
        for account in federal_account_set:
            account_title = account.account_title
            #print(account_title)

        # Use filter() instead of get() on Agency.objects
        # get() will likely raise an error on a bad agency id
        # Get the top_tier_agency_id from the Agency set
        top_tier_agency_id = Agency.objects.filter(
            toptier_agency__cgac_code=agency_identifier).first().toptier_agency_id
        #print(top_tier_agency_id)

        # The query is executed and cached.
        # for agency in agency_set:
            # subtier_agency_id = agency.subtier_agency_id

        # The cache is used for subsequent iteration.
        # for agency in agency_set:
        #    toptier_flag = agency.toptier_flag

        # if agency is None:
        #    return Agency.objects.none()
        # else:

        # Using final_objects below ensures that we're only pulling the latest
        # set of financial information for each fiscal year
        queryset = FinancialAccountsByProgramActivityObjectClass.final_objects.filter(
           treasury_account__agency_id=agency_identifier)
        print(queryset)
        queryset = queryset.annotate(
            major_object_class_name=F('object_class__major_object_class_name'),
            major_object_class_code=F('object_class__major_object_class')
        )
        print(queryset)
        # Sum and sort descending obligated_amount by object class
        queryset = queryset.values('major_object_class_name', 'major_object_class_code').annotate(
            obligated_amount=Sum('obligations_incurred_by_program_object_class_cpe')).order_by('-obligated_amount')
        print(queryset)
        # Return minor object class vars
        return queryset
        """
