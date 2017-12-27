from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from rest_framework.response import Response
from rest_framework_extensions.cache.decorators import cache_response
from usaspending_api.accounts.models import FederalAccount, TreasuryAppropriationAccount
from rest_framework.views import APIView
from django.db.models import Sum

def federal_account_filter(queryset, filter):
    for key, value in filter.items():
        if key == 'object_class':
            pass
        elif key == 'program_activity':
            pass
        elif key == 'time_period':
            pass
    return queryset


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
        return None


class SpendingOverTimeFederalAccountsViewSet(APIView):
    @cache_response()
    def post(self, request, pk, format=None):
        # create response
        response = {'results': {}}

        # get federal account id from url
        fa_id = int(pk)

        filters = request["filters"]
        time_period = request["time_period"]

        # get fin based on tas, select oc, make distinct values
        financial_account_queryset = \
            FinancialAccountsByProgramActivityObjectClass.objects.filter(treasury_account__federal_account_id=fa_id)

        filtered_fa = federal_account_filter(financial_account_queryset, filters).aggragete(
            outlay=Sum('gross_outlay_amount_by_tas_cpe'),
            obligations_incurred_filtered=Sum('obligations_incurred_total_by_tas_cpe')
        )

        unfiltered_fa = financial_account_queryset.aggragate(
            obligations_incurred_other=Sum('obligations_incurred_total_by_tas_cpe'),
            unobliged_balance=Sum('unobligated_balance_cpe')
        )

        result = {
            'outlay': filtered_fa['outlay'],                                                      # filter
            'obligations_incurred_filtered': filtered_fa['obligations_incurred_filtered'],        # filter
            'obligations_incurred_other': unfiltered_fa['obligations_incurred_other'],            # no filter
            'unobligated_balance': unfiltered_fa['unobligated_balance']                           # no filter
        }

        return Response({'results': result})


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