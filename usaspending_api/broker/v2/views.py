"""
These preliminary views are introduced strictly as examples of 
how to query from the models in broker/models.py - 
specifically, by using `.using('data_broker')`.

"""
from rest_framework import viewsets

from usaspending_api.broker.models import DetachedAwardProcurement, PublishedAwardFinancialAssistance
from usaspending_api.broker.serializers import DetachedAwardProcurementSerializer, PublishedAwardFinancialAssistanceSerializer
from rest_framework import viewsets


class DetachedAwardProcurementViewSet(viewsets.ModelViewSet):

    queryset = DetachedAwardProcurement.objects.using('data_broker').all()
    serializer_class = DetachedAwardProcurementSerializer


class PublishedAwardFinancialAssistanceViewSet(viewsets.ModelViewSet):

    queryset = PublishedAwardFinancialAssistance.objects.using('data_broker').all()
    serializer_class = PublishedAwardFinancialAssistanceSerializer
