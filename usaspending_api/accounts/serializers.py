from rest_framework import serializers

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.accounts.models import AppropriationAccountBalances, FederalAccount
from usaspending_api.financial_activities.serializers import FinancialAccountsByProgramActivityObjectClassSerializer
from usaspending_api.common.serializers import LimitableSerializer
from usaspending_api.references.serializers import RefProgramActivityBriefSerializer, ObjectClassBriefSerializer


class AppropriationAccountBalancesSerializer(LimitableSerializer):

    class Meta:

        model = AppropriationAccountBalances
        fields = '__all__'


class FederalAccountSerializer(LimitableSerializer):

    class Meta:

        model = FederalAccount
        fields = '__all__'


class TreasuryAppropriationAccountSerializer(LimitableSerializer):

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
            "program_balances": {
                "class": FinancialAccountsByProgramActivityObjectClassSerializer,
                "kwargs": {"read_only": True, "many": True}
            },
            "program_activities": {
                "class": RefProgramActivityBriefSerializer,
                "kwargs": {"read_only": True, "many": True}
            },
            "object_classes": {
                "class": ObjectClassBriefSerializer,
                "kwargs": {"read_only": True, "many": True}
            },
        }
