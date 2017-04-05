from rest_framework import serializers

from usaspending_api.accounts.models import (
    AppropriationAccountBalances, FederalAccount, TreasuryAppropriationAccount)
from usaspending_api.financial_activities.models import (
    FinancialAccountsByProgramActivityObjectClass
)
from usaspending_api.common.serializers import LimitableSerializer
from usaspending_api.references.serializers import (
    ProgramActivitySerializer, ObjectClassSerializer)


class AppropriationAccountBalancesSerializer(LimitableSerializer):

    class Meta:

        model = AppropriationAccountBalances
        fields = '__all__'


class FederalAccountSerializer(LimitableSerializer):

    class Meta:

        model = FederalAccount
        fields = '__all__'


class TasSerializer(LimitableSerializer):

    totals_program_activity = serializers.ListField()
    totals_object_class = serializers.ListField()
    totals = serializers.DictField()

    class Meta:

        model = TreasuryAppropriationAccount
        fields = '__all__'
        nested_serializers = {
            "federal_account": {
                "class": FederalAccountSerializer,
                "kwargs": {"read_only": True}
            },
            "account_balances": {
                "class": AppropriationAccountBalancesSerializer,
                "kwargs": {"read_only": True, "many": True}
            },
            "program_activities": {
                "class": ProgramActivitySerializer,
                "kwargs": {"read_only": True, "many": True}
            },
            "object_classes": {
                "class": ObjectClassSerializer,
                "kwargs": {"read_only": True, "many": True}
            },
        }


class TasCategorySerializer(LimitableSerializer):

    class Meta:

        model = FinancialAccountsByProgramActivityObjectClass
        fields = '__all__'
