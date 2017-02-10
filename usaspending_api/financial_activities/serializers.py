from usaspending_api.common.serializers import LimitableSerializer
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


class FinancialAccountsByProgramActivityObjectClassSerializer(LimitableSerializer):

    class Meta:

        model = FinancialAccountsByProgramActivityObjectClass
        fields = '__all__'
