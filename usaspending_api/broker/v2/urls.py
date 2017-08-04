from django.conf.urls import url
from rest_framework.routers import DefaultRouter

from usaspending_api.broker.v2 import views

broker_router = DefaultRouter()
broker_router.register('broker', views.DetachedAwardProcurementViewSet)
broker_router.register('broker', views.PublishedAwardFinancialAssistanceViewSet)

mode_list = {'get': 'list', 'post': 'list'}
mode_detail = {'get': 'retrieve', 'post': 'retrieve'}


urlpatterns = [
    url(r'^detachedawardprocurementviewset/$', views.DetachedAwardProcurementViewSet.as_view({'get': 'list'})),
    url(r'^publishedawardfinancialassistance/$', views.PublishedAwardFinancialAssistanceViewSet.as_view({'get': 'list'})),
]
