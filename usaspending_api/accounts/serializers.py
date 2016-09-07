from rest_framework import serializers
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.accounts.models import AppropriationAccountBalances

# Just some temporary serializers to test the database reflection
class TreasuryAppropriationAccountSerializer(serializers.ModelSerializer):

    class Meta:

        model = TreasuryAppropriationAccount
        fields = ('treasury_account_identifier', 'allocation_transfer_agency_id')


class AppropriationAccountBalancesSerializer(serializers.ModelSerializer):

    class Meta:

        model = AppropriationAccountBalances
        fields = ('budget_authority_unobligat_fyb', 'budget_authority_appropria_cpe')
