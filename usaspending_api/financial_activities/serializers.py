from rest_framework import serializers
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer


class FinancialAccountsByProgramActivityObjectClassSerializer(serializers.ModelSerializer):
    appropriation_account_balances = AppropriationAccountBalancesSerializer(read_only=True)

    class Meta:

        model = FinancialAccountsByProgramActivityObjectClass
        fields = '__all__'
