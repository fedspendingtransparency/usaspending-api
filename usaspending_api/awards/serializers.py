from rest_framework import serializers

from usaspending_api.awards.models import Award, FinancialAccountsByAwards, FinancialAccountsByAwardsTransactionObligations, FinancialAssistanceAward, Procurement
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer, TreasuryAppropriationAccountSerializer
from usaspending_api.common.serializers import LimitableSerializer
from usaspending_api.references.serializers import AgencySerializer, LegalEntitySerializer, LocationSerializer
from usaspending_api.common.helpers import fy


class FinancialAccountsByAwardsTransactionObligationsSerializer(LimitableSerializer):

    class Meta:
        model = FinancialAccountsByAwardsTransactionObligations
        fields = '__all__'


class ProcurementSerializer(LimitableSerializer):

    class Meta:
        model = Procurement
        fields = '__all__'


class FinancialAssistanceAwardSerializer(LimitableSerializer):

    class Meta:
        model = FinancialAssistanceAward
        fields = '__all__'


class FinancialAccountsByAwardsSerializer(LimitableSerializer):

    class Meta:
        model = FinancialAccountsByAwards
        fields = '__all__'
        nested_serializers = {
            "treasury_account": {
                "class": TreasuryAppropriationAccountSerializer,
                "kwargs": {"read_only": True}
            },
            "transaction_obligations": {
                "class": FinancialAccountsByAwardsTransactionObligationsSerializer,
                "kwargs": {"read_only": True, "many": True}
            },
        }


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
            "procurement_set": {
                "class": ProcurementSerializer,
                "kwargs": {"read_only": True, "many": True}
            },
            "financialassistanceaward_set": {
                "class": FinancialAssistanceAwardSerializer,
                "kwargs": {"read_only": True, "many": True}
            },
            "financial_set": {
                "class": FinancialAccountsByAwardsSerializer,
                "kwargs": {"read_only": True, "many": True}
            },
            "place_of_performance": {
                "class": LocationSerializer,
                "kwargs": {"read_only": True}
            },
        }

    date_signed__fy = serializers.SerializerMethodField()

    def get_date_signed__fy(self, obj):
        return fy(obj.date_signed)


class TransactionSerializer(LimitableSerializer):
    """Serializer transactions."""
    # Note: until we update the AwardAction abstract class to
    # a physical Transaction table, the transaction serializer's first
    # iteration will include procurement data only. This will let us
    # demo and test the endpoint while avoiding the work
    # of combining the separate procurement/assistance tables (that work
    # won't be needed once we make the AwardAction-->Transaction change).

    # The following code simulates having a single
    # Tranaction table with contract and assistance tables that can point back
    # to it and render as nested data in the transaction response.
    contract_data = serializers.SerializerMethodField()
    assistance_data = serializers.SerializerMethodField()

    def get_contract_data(self, procurement):
        return {
            "cost_or_pricing_data": procurement.cost_or_pricing_data,
            "naics": procurement.naics,
            "naics_description": procurement.naics_description,
            "product_or_service_code": procurement.product_or_service_code,
        }

    def get_assistance_data(self, procurement):
        return {}

    # End of temporary code to mock up nexted contract and assistance data.

    class Meta:

        model = Procurement
        # Manually setting the field list below to mock up the nested
        # contract and assistance data parts of the response
        fields = (
            'award', 'type', 'type_description', 'modification_number',
            'federal_action_obligation', 'action_date', 'description',
            'update_date', 'contract_data', 'assistance_data', 'type_of_contract_pricing',
            'type_of_contract_pricing_description')
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
            }
        }
