from usaspending_api.common.serializers import LimitableSerializer
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


class FinancialAccountsByProgramActivityObjectClassSerializer(LimitableSerializer):

    class Meta:

        model = FinancialAccountsByProgramActivityObjectClass
        nested_serializers = {
            "appropriation_account_balances": {
                "class": AppropriationAccountBalancesSerializer,
                "kwargs": {"read_only": True}
            },
        }
        fields = '__all__'
