from rest_framework import serializers

from usaspending_api.accounts.models import AppropriationAccountBalances, FederalAccount, TreasuryAppropriationAccount
from usaspending_api.common.serializers import LimitableSerializer
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.v1.serializers import (
    ProgramActivitySerializer,
    ObjectClassSerializer,
    ToptierAgencySerializer,
)
from usaspending_api.submissions.serializers import SubmissionAttributesSerializer


class FederalAccountSerializer(LimitableSerializer):
    class Meta:

        model = FederalAccount
        fields = "__all__"


class TasSerializer(LimitableSerializer):
    class Meta:

        model = TreasuryAppropriationAccount
        fields = "__all__"
        default_fields = [
            "treasury_account_identifier",
            "tas_rendering_label",
            "federal_account",
            "allocation_transfer_agency_id",
            "agency_id",
            "beginning_period_of_availability",
            "ending_period_of_availability",
            "availability_type_code",
            "main_account_code",
            "sub_account_code",
            "account_title",
            "reporting_agency_id",
            "reporting_agency_name",
            "budget_bureau_code",
            "budget_bureau_name",
            "fr_entity_code",
            "fr_entity_description",
            "budget_function_code",
            "budget_function_title",
            "budget_subfunction_code",
            "budget_subfunction_title",
            "account_balances",
            "program_balances",
            "program_activities",
            "object_classes",
            "totals_program_activity",
            "totals_object_class",
            "totals",
            "funding_toptier_agency",
            "awarding_toptier_agency",
        ]
        nested_serializers = {
            "federal_account": {"class": FederalAccountSerializer, "kwargs": {"read_only": True}},
            "awarding_toptier_agency": {"class": ToptierAgencySerializer, "kwargs": {"read_only": True}},
            "funding_toptier_agency": {"class": ToptierAgencySerializer, "kwargs": {"read_only": True}},
        }


class AppropriationAccountBalancesSerializer(LimitableSerializer):
    class Meta:

        model = AppropriationAccountBalances
        fields = "__all__"
        nested_serializers = {
            "treasury_account_identifier": {"class": TasSerializer, "kwargs": {"read_only": True}},
            "submission": {"class": SubmissionAttributesSerializer, "kwargs": {"read_only": True}},
        }


class TasCategorySerializer(LimitableSerializer):
    class Meta:

        model = FinancialAccountsByProgramActivityObjectClass
        fields = "__all__"
        nested_serializers = {
            "program_activity": {"class": ProgramActivitySerializer, "kwargs": {"read_only": True}},
            "object_class": {"class": ObjectClassSerializer, "kwargs": {"read_only": True}},
            "treasury_account": {"class": TasSerializer, "kwargs": {"read_only": True}},
            "submission": {"class": SubmissionAttributesSerializer, "kwargs": {"read_only": True}},
        }


class MinorObjectClassFinancialSpendingSerializer(serializers.Serializer):

    object_class_code = serializers.CharField()
    object_class_name = serializers.CharField()
    obligated_amount = serializers.DecimalField(None, 2)


class ObjectClassFinancialSpendingSerializer(serializers.Serializer):

    major_object_class_code = serializers.CharField()
    major_object_class_name = serializers.CharField()
    obligated_amount = serializers.DecimalField(None, 2)


class AgenciesFinancialBalancesSerializer(serializers.Serializer):

    budget_authority_amount = serializers.DecimalField(None, 2)
    obligated_amount = serializers.DecimalField(None, 2)
    outlay_amount = serializers.DecimalField(None, 2)


class BudgetAuthoritySerializer(serializers.Serializer):

    year = serializers.IntegerField()
    total = serializers.IntegerField()


class FederalAccountByObligationSerializer(serializers.Serializer):

    id = serializers.CharField()
    account_title = serializers.CharField()
    account_number = serializers.CharField()
    obligated_amount = serializers.DecimalField(None, 2)
