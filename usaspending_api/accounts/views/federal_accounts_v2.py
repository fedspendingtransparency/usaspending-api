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

        # get fin based on tas, select oc, make distict values
        # add conditions since '__in' will not work with non-querysets
        financial_account_queryset = \
            FinancialAccountsByProgramActivityObjectClass.objects.filter(treasury_account__in=tas_queryset) \
            .select_related('object_class').distinct('object_class') \
            if tas_queryset.count() >=1 \
            else FinancialAccountsByProgramActivityObjectClass.objects.filter(treasury_account=tas_queryset) \
            .select_related('object_class').distinct('object_class')

        # Retrieve only unique major class ids and names
        major_classes = set([(obj.object_class.major_object_class, obj.object_class.major_object_class_name)
                        for obj in financial_account_queryset])
        result = [
            {
                'id': int(maj[0]),
                'name': maj[1],
                'minor_object_class': 
                [
                    {'id': obj[0], 'name': obj[1]}
                    for obj in set([(oc.object_class.object_class, oc.object_class.object_class_name)
                    for oc in financial_account_queryset if oc.object_class.major_object_class == maj[0]])
                ]
            }
            for maj in major_classes
        ]
        return Response({'results': result})
