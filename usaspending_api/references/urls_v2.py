from django.conf.urls import url
from usaspending_api.references import views
from usaspending_api.references.views_v2 import agency
from rest_framework.routers import DefaultRouter

guide_router = DefaultRouter()
guide_router.register('guide', views.GuideViewSet)

mode_list = {'get': 'list', 'post': 'list'}
mode_detail = {'get': 'retrieve', 'post': 'retrieve'}


urlpatterns = [
    url(r'^agency/(?P<pk>[0-9]+)/$', agency.AgencyViewSet.as_view())
]
