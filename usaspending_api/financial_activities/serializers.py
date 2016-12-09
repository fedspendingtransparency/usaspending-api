from usaspending_api.common.serializers import LimitableSerializer
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


class FinancialAccountsByProgramActivityObjectClassSerializer(LimitableSerializer):
    appropriation_account_balances = AppropriationAccountBalancesSerializer(read_only=True)

    class Meta:

        model = FinancialAccountsByProgramActivityObjectClass
        fields = '__all__'
