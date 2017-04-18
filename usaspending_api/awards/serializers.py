from rest_framework import serializers

from usaspending_api.awards.models import (
    Award, FinancialAccountsByAwards,
    Transaction, TransactionAssistance, TransactionContract, Subaward)
from usaspending_api.accounts.serializers import TasSerializer
from usaspending_api.common.serializers import LimitableSerializer
from usaspending_api.references.serializers import ProgramActivitySerializer, ObjectClassSerializer
from usaspending_api.references.serializers import AgencySerializer, LegalEntitySerializer, LocationSerializer, CfdaSerializer
from usaspending_api.common.helpers import fy


class FinancialAccountsByAwardsSerializer(LimitableSerializer):

    class Meta:
        model = FinancialAccountsByAwards
        fields = '__all__'
        nested_serializers = {
            "treasury_account": {
                "class": TasSerializer,
                "kwargs": {"read_only": True}
            },
            "program_activity": {
                "class": ProgramActivitySerializer,
                "kwargs": {"read_only": True}
            },
            "object_class": {
                "class": ObjectClassSerializer,
                "kwargs": {"read_only": True}
            },
        }


class TransactionAssistanceSerializer(LimitableSerializer):

    class Meta:
        model = TransactionAssistance
        fields = '__all__'

        nested_serializers = {
            "cfda": {
                "class": CfdaSerializer,
                "kwargs": {"read_only": True}
            },
        }


class TransactionContractSerializer(LimitableSerializer):

    class Meta:
        model = TransactionContract
        fields = '__all__'


class TransactionSerializer(LimitableSerializer):
    """Serialize complete transactions, including assistance and contract data."""

    class Meta:

        model = Transaction
        fields = '__all__'

        nested_serializers = {
            # name below must match related_name in TransactionAssistance
            "assistance_data": {
                "class": TransactionAssistanceSerializer,
                "kwargs": {"read_only": True}
            },
            # name below must match related_name in TransactionContract
            "contract_data": {
                "class": TransactionContractSerializer,
                "kwargs": {"read_only": True}
            },
            "recipient": {
                "class": LegalEntitySerializer,
                "kwargs": {"read_only": True}
            },
            "awarding_agency": {
                "class": AgencySerializer,
                "kwargs": {"read_only": True}
            },
            "funding_agency": {
                "class": AgencySerializer,
                "kwargs": {"read_only": True}
            },
            "place_of_performance": {
                "class": LocationSerializer,
                "kwargs": {"read_only": True}
            }
        }


class SubawardSerializer(LimitableSerializer):

    class Meta:
        model = Subaward
        fields = '__all__'


class AwardSerializer(LimitableSerializer):

    class Meta:

        model = Award
        fields = '__all__'
        nested_serializers = {
            "recipient": {
                "class": LegalEntitySerializer,
                "kwargs": {"read_only": True}
            },
            "awarding_agency": {
                "class": AgencySerializer,
                "kwargs": {"read_only": True}
            },
            "funding_agency": {
                "class": AgencySerializer,
                "kwargs": {"read_only": True}
            },
            "place_of_performance": {
                "class": LocationSerializer,
                "kwargs": {"read_only": True}
            },
            "latest_transaction": {
                "class": TransactionSerializer,
                "kwargs": {"read_only": True}
            }
        }

    date_signed__fy = serializers.SerializerMethodField()

    def get_date_signed__fy(self, obj):
        return fy(obj.date_signed)
