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
        default_fields = [
            "financial_accounts_by_awards_id",
            "award",
            "treasury_account",
            "transaction_obligated_amount",
            "object_class",
            "program_activity",
            "piid",
            "fain",
            "uri",
            "gross_outlay_amount_by_award_cpe",
            "gross_outlay_amount_by_award_fyb",
            "certified_date",
            "last_modified_date"
        ]
        nested_serializers = {
            "treasury_account": {
                "class": TasSerializer,
                "kwargs": {
                    "read_only": True,
                    "default_fields": [
                        "treasury_account_identifier",
                        "tas_rendering_label",
                        "account_title",
                        "reporting_agency_id",
                        "reporting_agency_name",
                        "federal_account",
                        "funding_toptier_agency",
                        "awarding_toptier_agency"
                    ]
                }
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
        default_fields = [
            "fain",
            "uri",
            "cfda",
            "cfda_number",
            "cfda_title",
            "face_value_loan_guarantee",
            "original_loan_subsidy_cost",
            "type"
        ]
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
        default_fields = [
            "piid",
            "parent_award_id",
            "type",
            "type_description",
            "cost_or_pricing_data",
            "type_of_contract_pricing",
            "type_of_contract_pricing_description",
            "naics",
            "naics_description",
            "product_or_service_code"
        ]


class TransactionSerializer(LimitableSerializer):
    """Serialize complete transactions, including assistance and contract data."""

    class Meta:

        model = Transaction
        fields = '__all__'
        default_fields = [
            "id",
            "type",
            "type_description",
            "period_of_performance_start_date",
            "period_of_performance_current_end_date",
            "action_date",
            "action_type",
            "action_type_description",
            "action_date__fy",
            "federal_action_obligation",
            "modification_number",
            "awarding_agency",
            "funding_agency",
            "recipient",
            "description",
            "place_of_performance",
            "contract_data",  # must match related_name in TransactionContract
            "assistance_data"  # must match related_name in TransactionAssistance
        ]
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
            "cfda": {
                "class": CfdaSerializer,
                "kwargs": {"read_only": True}
            },
        }


class AwardSerializer(LimitableSerializer):

    class Meta:

        model = Award
        fields = '__all__'
        default_fields = [
            "id",
            "type",
            "type_description",
            "category",
            "total_obligation",
            "total_outlay",
            "date_signed",
            "description",
            "piid",
            "fain",
            "uri",
            "period_of_performance_start_date",
            "period_of_performance_current_end_date",
            "potential_total_value_of_award",
            "place_of_performance",
            "awarding_agency",
            "funding_agency",
            "recipient",
            "date_signed__fy",
            "subaward_count",
            "total_subaward_amount"
        ]
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
