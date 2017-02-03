from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.common.serializers import LimitableSerializer


class TreasuryAppropriationAccountSerializer(LimitableSerializer):

    class Meta:

        model = TreasuryAppropriationAccount
        fields = '__all__'


class AppropriationAccountBalancesSerializer(LimitableSerializer):

    class Meta:

        model = AppropriationAccountBalances
        nested_serializers = {
            "treasury_account_identifier": {
                "class": TreasuryAppropriationAccountSerializer,
                "kwargs": {"read_only": True}
            },
        }
        fields = '__all__'
