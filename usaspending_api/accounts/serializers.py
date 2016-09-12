from rest_framework import serializers
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.accounts.models import AppropriationAccountBalances

# Just some temporary serializers to test the database reflection
class TreasuryAppropriationAccountSerializer(serializers.ModelSerializer):

    class Meta:

        model = TreasuryAppropriationAccount
        fields = '__all__'


class AppropriationAccountBalancesSerializer(serializers.ModelSerializer):
    treasury_account_identifier = TreasuryAppropriationAccountSerializer(read_only=True)
    class Meta:

        model = AppropriationAccountBalances
        fields = '__all__'
