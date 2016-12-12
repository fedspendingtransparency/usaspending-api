from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.common.serializers import LimitableSerializer


class TreasuryAppropriationAccountSerializer(LimitableSerializer):

    class Meta:

        model = TreasuryAppropriationAccount
        fields = '__all__'


class AppropriationAccountBalancesSerializer(LimitableSerializer):
    treasury_account_identifier = TreasuryAppropriationAccountSerializer(read_only=True)

    class Meta:

        model = AppropriationAccountBalances
        fields = '__all__'
