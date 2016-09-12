from rest_framework import serializers
from usaspending_api.awards.models import *
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer


class FinancialAccountsByAwardsSerializer(serializers.ModelSerializer):
    appropriation_account_balances = AppropriationAccountBalancesSerializer(read_only=True)

    class Meta:
        model = FinancialAccountsByAwards
        fields = '__all__'


class FinancialAccountsByAwardsTransactionObligationsSerializer(serializers.ModelSerializer):
    financial_accounts_by_awards = FinancialAccountsByAwardsSerializer(read_only=True)

    class Meta:
        model = FinancialAccountsByAwardsTransactionObligations
        fields = '__all__'
