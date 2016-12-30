from rest_framework import serializers
from usaspending_api.awards.models import *
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer
from usaspending_api.references.serializers import *
from usaspending_api.common.serializers import LimitableSerializer


class FinancialAccountsByAwardsSerializer(LimitableSerializer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.map_nested_serializer(
            'appropriation_account_balances', AppropriationAccountBalancesSerializer)

    class Meta:
        model = FinancialAccountsByAwards
        fields = '__all__'


class FinancialAccountsByAwardsTransactionObligationsSerializer(LimitableSerializer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.map_nested_serializer(
            'financial_accounts_by_awards', FinancialAccountsByAwardsSerializer)

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

    # recipient = LegalEntitySerializer(read_only=True)
    # awarding_agency = AgencySerializer(read_only=True)
    # funding_agency = AgencySerializer(read_only=True)
    # procurement_set = ProcurementSerializer(many=True, read_only=True)
    # financialassistanceaward_set = FinancialAssistanceAwardSerializer(many=True, read_only=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.map_nested_serializer(
            'recipient', LegalEntitySerializer)
        self.map_nested_serializer(
            'awarding_agency', AgencySerializer)
        self.map_nested_serializer(
            'funding_agency', AgencySerializer)
        self.map_nested_serializer(
            'procurement_set', ProcurementSerializer)
        self.map_nested_serializer(
            'financialassistanceaward_set', FinancialAssistanceAwardSerializer(
                read_only=True, many=True, context=self.context))

    class Meta:

        model = Award
        fields = '__all__'
