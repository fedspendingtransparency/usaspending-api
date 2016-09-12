from rest_framework import serializers
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


class FinancialAccountsByProgramActivityObjectClassSerializer(serializers.ModelSerializer):

    class Meta:

        model = FinancialAccountsByProgramActivityObjectClass
        fields = '__all__'

        
