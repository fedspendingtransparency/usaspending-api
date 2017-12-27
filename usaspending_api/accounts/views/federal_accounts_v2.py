from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from rest_framework.response import Response
from rest_framework_extensions.cache.decorators import cache_response
from usaspending_api.accounts.models import FederalAccount, TreasuryAppropriationAccount
from rest_framework.views import APIView


class ObjectClassFederalAccountsViewSet(APIView):
    """Returns financial spending data by object class."""

    @cache_response()
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

        # get tas related to FA
        tas_ids = TreasuryAppropriationAccount.objects.filter(federal_account=fa) \
            .values_list('treasury_account_identifier', flat=True)

        # get fin based on tas, select oc, make distinct values
        financial_account_queryset = \
            FinancialAccountsByProgramActivityObjectClass.objects.filter(treasury_account__in=tas_ids) \
            .select_related('object_class').distinct('object_class')

        # Retrieve only unique major class ids and names
        major_classes = set([(obj.object_class.major_object_class, obj.object_class.major_object_class_name)
                             for obj in financial_account_queryset])
        result = [
            {
                'id': maj[0],
                'name': maj[1],
                'minor_object_class':
                    [
                        {'id': obj[0], 'name': obj[1]}
                        for obj in set([(oc.object_class.object_class, oc.object_class.object_class_name)
                                        for oc in financial_account_queryset
                                        if oc.object_class.major_object_class == maj[0]])
                    ]
            }
            for maj in major_classes
        ]
        return Response({'results': result})


class DescriptionFederalAccountsViewSet(APIView):
    @cache_response()
    def get(self, request, pk, format=None):
        return None


class FiscalYearSnapshotFederalAccountsViewSet(APIView):
    @cache_response()
    def get(self, request, pk, format=None):
        result = {
            "results": {
                "outlay": 1,
                "budget_authority": 1,
                "obligated": 1,
                "unobligated": 1,
                "balance_brought_forward": 1,
                "other_budgetary_resources": 1,
                "appropriations": 1
            }
        }
        return Response(result)


class SpendingOverTimeFederalAccountsViewSet(APIView):
    @cache_response()
    def get(self, request, pk, format=None):
        return None


class SpendingByCategoryFederalAccountsViewSet(APIView):
    @cache_response()
    def get(self, request, pk, format=None):
        return None


class SpendingByAwardCountFederalAccountsViewSet(APIView):
    @cache_response()
    def get(self, request, pk, format=None):
        return None


class SpendingByAwardFederalAccountsViewSet(APIView):
    @cache_response()
    def get(self, request, pk, format=None):
        return None