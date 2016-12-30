from usaspending_api.common.serializers import LimitableSerializer
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


class FinancialAccountsByProgramActivityObjectClassSerializer(LimitableSerializer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.map_nested_serializer(
            'appropriation_account_balances', AppropriationAccountBalancesSerializer)

    class Meta:

        model = FinancialAccountsByProgramActivityObjectClass
        fields = '__all__'
