from rest_framework.serializers import Serializer

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


class AwardActionSerializer(LimitableSerializer):

    class Meta:
        model = AwardAction
        fields = '__all__'


class AwardIdObjectSerializer(Serializer):
    piid = serializers.CharField()
    fain = serializers.CharField()
    uri = serializers.CharField()


class TopLevelObjecttSerializer(Serializer):
    #id = serializers.IntegerField
    type = serializers.CharField()
    total_obligation = serializers.DecimalField(max_digits=15, decimal_places=2)
    total_outlay = serializers.DecimalField(max_digits=15, decimal_places=2)
    date_signed = serializers.DateField()
    description = serializers.CharField()
    period_of_performance_start_date = serializers.DateField()
    period_of_performance_current_end_date = serializers.DateField()
    update_date = serializers.DateTimeField()


##AJ Added toplevel_object, Need to Add Location & AwardAction
class AwardSerializer(LimitableSerializer):
    toplevel_object = TopLevelObjecttSerializer()
    procurement_set = ProcurementSerializer(many=True, read_only=True)
    financialassistanceaward_set = FinancialAssistanceAwardSerializer(many=True, read_only=True)
    award_id_object = AwardIdObjectSerializer()
    recipient = LegalEntitySerializer(read_only=True)
    ##Location = LocationSerializer(read_only=True)
    awarding_agency = AgencySerializer(read_only=True)
    funding_agency = AgencySerializer(read_only=True)
    ##AwardAction = AwardActionSerializer(read_only=True)

    class Meta:
        model = Award
        fields = '__all__'
