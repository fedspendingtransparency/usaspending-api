from rest_framework import serializers
from usaspending_api.awards.models import *
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer
from usaspending_api.references.serializers import *


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


class AwardSerializer(serializers.ModelSerializer):

    recipient = LegalEntitySerializer(read_only=True)
    awarding_agency = AgencySerializer(read_only=True)
    funding_agency = AgencySerializer(read_only=True)

    class Meta:

        model = Award
        fields = '__all__'
