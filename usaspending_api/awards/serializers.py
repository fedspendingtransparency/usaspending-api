from rest_framework import serializers
from usaspending_api.awards.models import FinancialAccountsByAwardsTransactionObligations


class FinancialAccountsByAwardsTransactionObligationsSerializer(serializers.ModelSerializer):

    class Meta:
        model = FinancialAccountsByAwardsTransactionObligations
        fields = '__all__'
