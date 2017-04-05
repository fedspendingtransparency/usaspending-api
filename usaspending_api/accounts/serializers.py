from rest_framework import serializers

from usaspending_api.accounts.models import (
    AppropriationAccountBalances, FederalAccount, TreasuryAppropriationAccount)
from usaspending_api.financial_activities.models import (
    FinancialAccountsByProgramActivityObjectClass
)
from usaspending_api.common.serializers import LimitableSerializer
from usaspending_api.references.serializers import (
    ProgramActivitySerializer, ObjectClassSerializer)
from usaspending_api.submissions.serializers import SubmissionAttributesSerializer

class AppropriationAccountBalancesSerializer(LimitableSerializer):

    class Meta:

        model = AppropriationAccountBalances
        fields = '__all__'


class FederalAccountSerializer(LimitableSerializer):

    class Meta:

        model = FederalAccount
        fields = '__all__'


class TasSerializer(LimitableSerializer):

    class Meta:

        model = TreasuryAppropriationAccount
        fields = '__all__'
        nested_serializers = {
            "federal_account": {
                "class": FederalAccountSerializer,
                "kwargs": {"read_only": True}
            }
        }


class TasCategorySerializer(LimitableSerializer):

    class Meta:

        model = FinancialAccountsByProgramActivityObjectClass
        fields = '__all__'
        nested_serializers = {
            "program_activity": {
                "class": ProgramActivitySerializer,
                "kwargs": {"read_only": True}
            },
            "object_class": {
                "class": ObjectClassSerializer,
                "kwargs": {"read_only": True}
            },
            "treasury_account": {
                "class": TasSerializer,
                "kwargs": {"read_only": True}
            },
            "submission": {
                "class": SubmissionAttributesSerializer,
                "kwargs": {"read_only": True}
            }
        }
