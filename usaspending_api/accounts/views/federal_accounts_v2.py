from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from rest_framework.response import Response
from usaspending_api.accounts.models import FederalAccount, TreasuryAppropriationAccount
from rest_framework.views import APIView


class ObjectClassFederalAccountsViewSet(APIView):
    """Returns financial spending data by object class."""

    def get(self, request, pk, format=None):
        """Return the view's queryset."""
        # create response
        response = {'results': {}}

        # get federal account id from url
        fa_id = int(pk)

        # get FA row
        fa = FederalAccount.objects.filter(id=fa_id).first()
        if fa is None:
            return Response(response)

        # get list of relevant object classes to the FA
        oc_list = []

        # get tas related to FA
        tas_queryset = TreasuryAppropriationAccount.objects.filter(federal_account=fa)

        # get fin based on tas
        for tas in tas_queryset:
            financial_account_queryset = \
                FinancialAccountsByProgramActivityObjectClass.objects.filter(treasury_account=tas)

            # get oc out of financial account
            for financial_account in financial_account_queryset:
                oc_list.append(financial_account.object_class)

        # remove non-uniques
        oc_set = set(oc_list)

        # structure response
        major_classes = set(oc.major_object_class_name for oc in oc_set)
        result = [
            {
                'major_object_class': maj,
                'minor_object_class': [oc.object_class_name for oc in oc_set if oc.major_object_class_name == maj]
            }
            for maj in major_classes
        ]
        return Response({'results': result})
