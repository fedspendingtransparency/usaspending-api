from django.conf.urls import url
from rest_framework.routers import DefaultRouter

from usaspending_api.references.v1 import views
from usaspending_api.references.v2.views import agency, toptier_agencies, data_dictionary, glossary


# This does not appear to be used and looks like its an artifact from a copy-pasta of v1 urls. Can probably delete?
glossary_router = DefaultRouter()
glossary_router.register('glossary', views.GlossaryViewSet)

mode_list = {'get': 'list', 'post': 'list'}
mode_detail = {'get': 'retrieve', 'post': 'retrieve'}


urlpatterns = [
    url(r'^agency/(?P<pk>[0-9]+)/$', agency.AgencyViewSet.as_view()),
    url(r'^toptier_agencies/$', toptier_agencies.ToptierAgenciesViewSet.as_view()),
    url(r'^data_dictionary/$', data_dictionary.DataDictionaryViewSet.as_view()),
    url(r'^glossary/$', glossary.GlossaryViewSet.as_view()),
]
