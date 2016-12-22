from usaspending_api.awards.models import *
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer
from usaspending_api.references.serializers import *
from usaspending_api.common.serializers import LimitableSerializer


class FinancialAccountsByAwardsSerializer(serializers.ModelSerializer):

    appropriation_account_balances = AppropriationAccountBalancesSerializer(read_only=True)

    class Meta:
        model = FinancialAccountsByAwards
        fields = '__all__'


class FinancialAccountsByAwardsTransactionObligationsSerializer(LimitableSerializer):

    financial_accounts_by_awards = FinancialAccountsByAwardsSerializer(read_only=True)

    class Meta:
        model = FinancialAccountsByAwardsTransactionObligations
        fields = '__all__'


class ProcurementSerializer(LimitableSerializer): #everything should be a LimitableSerializer

    class Meta:
        model = Procurement
        fields = '__all__'


class FinancialAssistanceAwardSerializer(serializers.ModelSerializer):

    class Meta:
        model = FinancialAssistanceAward
        fields = '__all__'


class AwardSerializer(LimitableSerializer):

    recipient = LegalEntitySerializer(read_only=True)
    awarding_agency = AgencySerializer(read_only=True)
    funding_agency = AgencySerializer(read_only=True)
    procurement_set = ProcurementSerializer(many=True, read_only=True)
    financialassistanceaward_set = FinancialAssistanceAwardSerializer(many=True, read_only=True)

    class Meta:

        model = Award
        fields = '__all__'
