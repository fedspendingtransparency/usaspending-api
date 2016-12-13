from rest_framework import serializers
from usaspending_api.common.serializers import LimitableSerializer
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


class FinancialAccountsByProgramActivityObjectClassSerializer(LimitableSerializer):
    appropriation_account_balances = serializers.SerializerMethodField()

    def get_appropriation_account_balances(self, obj):
        return self.get_subserializer_data(AppropriationAccountBalancesSerializer, obj.appropriation_account_balances, 'appropriation_account_balances', read_only=True)

    class Meta:

        model = FinancialAccountsByProgramActivityObjectClass
        fields = '__all__'
