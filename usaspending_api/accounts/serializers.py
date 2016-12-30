from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.common.serializers import LimitableSerializer


class TreasuryAppropriationAccountSerializer(LimitableSerializer):

    class Meta:

        model = TreasuryAppropriationAccount
        fields = '__all__'


class AppropriationAccountBalancesSerializer(LimitableSerializer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.map_nested_serializer(
            'treasury_account_identifier', TreasuryAppropriationAccountSerializer)

    class Meta:

        model = AppropriationAccountBalances
        fields = '__all__'
