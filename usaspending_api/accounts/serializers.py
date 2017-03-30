from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.accounts.models import AppropriationAccountBalances, FederalAccount
from usaspending_api.financial_activities.serializers import FinancialAccountsByProgramActivityObjectClassSerializer
from usaspending_api.common.serializers import LimitableSerializer


class AppropriationAccountBalancesSerializer(LimitableSerializer):

    class Meta:

        model = AppropriationAccountBalances
        fields = '__all__'


class FederalAccountSerializer(LimitableSerializer):

    class Meta:

        model = FederalAccount
        fields = '__all__'


class TreasuryAppropriationAccountSerializer(LimitableSerializer):

    class Meta:

        model = TreasuryAppropriationAccount
        fields = '__all__'
        nested_serializers = {
            "federal_account": {
                "class": FederalAccountSerializer,
                "kwargs": {"read_only": True}
            },
            "account_balances": {
                "class": AppropriationAccountBalancesSerializer,
                "kwargs": {"read_only": True, "many": True}
            },
            "program_balances": {
                "class": FinancialAccountsByProgramActivityObjectClassSerializer,
                "kwargs": {"read_only": True, "many": True}
            }
        }
