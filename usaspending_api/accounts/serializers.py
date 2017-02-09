from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.common.serializers import LimitableSerializer


class AppropriationAccountBalancesSerializer(LimitableSerializer):

    class Meta:

        model = AppropriationAccountBalances
        fields = '__all__'


class TreasuryAppropriationAccountSerializer(LimitableSerializer):

    class Meta:

        model = TreasuryAppropriationAccount
        fields = '__all__'
        nested_serializers = {
            "account_balances": {
                "class": AppropriationAccountBalancesSerializer,
                "kwargs": {"read_only": True, "many": True}
            }
        }
