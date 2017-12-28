from datetime import datetime
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.common.helpers import fy
from rest_framework.response import Response
from rest_framework_extensions.cache.decorators import cache_response
from usaspending_api.accounts.models import FederalAccount, TreasuryAppropriationAccount, AppropriationAccountBalances
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

        queryset = AppropriationAccountBalances.objects.filter(treasury_account_identifier__federal_account_id=int(
            pk)).filter(final_of_fy=True).filter(submission__reporting_fiscal_year=fy(datetime.today()))
        rq = queryset.first()

        if rq:
            result = {
                "results": {
                    "outlay":
                    rq.gross_outlay_amount_by_tas_cpe,
                    "budget_authority":
                    rq.budget_authority_available_amount_total_cpe,
                    "obligated":
                    rq.obligations_incurred_total_by_tas_cpe,
                    "unobligated":
                    rq.unobligated_balance_cpe,
                    "balance_brought_forward":
                    rq.budget_authority_unobligated_balance_brought_forward_fyb
                    +
                    rq.adjustments_to_unobligated_balance_brought_forward_cpe,
                    "other_budgetary_resources":
                    rq.other_budgetary_resources_amount_cpe,
                    "appropriations":
                    rq.budget_authority_appropriated_amount_cpe
                }
            }
        else:
            result = {
                "results": {
                    "outlay": 0,
                    "budget_authority": 0,
                    "obligated": 0,
                    "unobligated": 0,
                    "balance_brought_forward": 0,
                    "other_budgetary_resources": 0,
                    "appropriations": 0,
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
