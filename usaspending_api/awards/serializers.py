from rest_framework import serializers
from usaspending_api.awards.models import *
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer
from usaspending_api.references.serializers import *
from usaspending_api.common.serializers import LimitableSerializer


class FinancialAccountsByAwardsSerializer(LimitableSerializer):

    appropriation_account_balances = serializers.SerializerMethodField()

    def get_appropriation_account_balances(self, obj):
        return self.get_subserializer_data(AppropriationAccountBalancesSerializer, obj.appropriation_account_balances, 'appropriation_account_balances', read_only=True)

    class Meta:
        model = FinancialAccountsByAwards
        fields = '__all__'


class FinancialAccountsByAwardsTransactionObligationsSerializer(LimitableSerializer):

    financial_accounts_by_awards = serializers.SerializerMethodField()

    def get_financial_accounts_by_awards(self, obj):
        return self.get_subserializer_data(FinancialAccountsByAwardsSerializer, obj.financial_accounts_by_awards, 'financial_accounts_by_awards', read_only=True)

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

    recipient = serializers.SerializerMethodField()
    awarding_agency = serializers.SerializerMethodField()
    funding_agency = serializers.SerializerMethodField()
    procurement_set = serializers.SerializerMethodField()
    financialassistanceaward_set = serializers.SerializerMethodField()

    def get_recipient(self, obj):
        return self.get_subserializer_data(LegalEntitySerializer, obj.recipient, 'recipient', read_only=True)

    def get_funding_agency(self, obj):
        return self.get_subserializer_data(AgencySerializer, obj.funding_agency, 'funding_agency', read_only=True)

    def get_awarding_agency(self, obj):
        return self.get_subserializer_data(AgencySerializer, obj.awarding_agency, 'awarding_agency', read_only=True)

    def get_procurement_set(self, obj):
        return self.get_subserializer_data(ProcurementSerializer, obj.procurement_set.all(), 'procurement_set', many=True, read_only=True)

    def get_financialassistanceaward_set(self, obj):
        return self.get_subserializer_data(FinancialAssistanceAwardSerializer, obj.financialassistanceaward_set, 'financialassistanceaward_set', many=True, read_only=True)

    class Meta:

        model = Award
        fields = '__all__'
