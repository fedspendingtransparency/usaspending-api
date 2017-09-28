from usaspending_api.broker.models_old import DetachedAwardProcurement, PublishedAwardFinancialAssistance
from usaspending_api.common.serializers import LimitableSerializer


class DetachedAwardProcurementSerializer(LimitableSerializer):
    class Meta:
        model = DetachedAwardProcurement


class PublishedAwardFinancialAssistanceSerializer(LimitableSerializer):
    class Meta:
        model = PublishedAwardFinancialAssistance
