from rest_framework import serializers
from usaspending_api.awards.models import *
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer
from usaspending_api.references.serializers import *
from usaspending_api.common.serializers import LimitableSerializer


class FinancialAccountsByAwardsSerializer(LimitableSerializer):

    appropriation_account_balances = AppropriationAccountBalancesSerializer(read_only=True)

    class Meta:
        model = FinancialAccountsByAwards
        fields = '__all__'


class FinancialAccountsByAwardsTransactionObligationsSerializer(LimitableSerializer):

    financial_accounts_by_awards = FinancialAccountsByAwardsSerializer(read_only=True)

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
        }
