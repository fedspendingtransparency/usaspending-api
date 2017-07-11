from django.db.models import F, Sum
from usaspending_api.accounts.serializers import (ObjectClassFinancialSpendingSerializer,
                                                  MinorObjectClassFinancialSpendingSerializer)
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import Agency
from usaspending_api.common.views import DetailViewSet
from usaspending_api.common.exceptions import InvalidParameterException
from rest_framework.response import Response
from usaspending_api.accounts.models import FederalAccount, TreasuryAppropriationAccount


class ObjectClassFederalAccountsViewSet(DetailViewSet):
    """Returns financial spending data by object class."""

    def get(self, request, pk, format=None):
        """Return the view's queryset."""
        # create response
        response = {'results': {}}

        # get federal account id from url
        fa_id = int(pk)

        # get FA row
        fa = FederalAccount.objects.filter(agency_identifier=fa_id).first()
        if fa is None:
            return Response(response)

        # get list of relevant object classes to the FA
        oc_list = []

        # get tas related to FA
        tas_queryset = TreasuryAppropriationAccount.objects.filter(federal_account=fa)

        # get fin based on tas
        for tas in tas_queryset:
            financial_account_queryset = FinancialAccountsByProgramActivityObjectClass.objects.filter(tas=tas)

            # get oc out of financial account
            for financial_account in financial_account_queryset:
                oc_list.append(financial_account.object_class)

        # remove non-uniques
        oc_set = set(oc_list)

        # structure response
        ret_val = {}

        for oc in oc_set:
            if ret_val.contains({'major_object_class': oc.major_object_class_name}):
                # add to object class
                pass
            else:
                ret_val.append({'major_object_class': oc.major_object_class_name, 'minor_object_class': [oc.object_class_name]})

        return Response({'results': ret_val})
