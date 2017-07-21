from django.conf.urls import url
from rest_framework.routers import DefaultRouter

from usaspending_api.references.v1 import views
from usaspending_api.references.v2.views import agency, toptier_agencies

glossary_router = DefaultRouter()
glossary_router.register('glossary', views.GlossaryViewSet)

mode_list = {'get': 'list', 'post': 'list'}
mode_detail = {'get': 'retrieve', 'post': 'retrieve'}


urlpatterns = [
    url(r'^agency/(?P<pk>[0-9]+)/$', agency.AgencyViewSet.as_view()),
    url(r'^toptier_agencies/$', toptier_agencies.ToptierAgenciesViewSet.as_view())
]
