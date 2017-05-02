from django.conf.urls import url, include
from usaspending_api.references import views
from rest_framework.routers import DefaultRouter

guide_router = DefaultRouter()
guide_router.register('guide', views.GuideViewSet)

mode_list = {'get': 'list', 'post': 'list'}
mode_detail = {'get': 'retrieve', 'post': 'retrieve'}


urlpatterns = [
    url(r'', include(guide_router.urls)),
    url(r'^locations/$', views.LocationEndpoint.as_view(mode_list)),
    url(r'^locations/geocomplete', views.LocationGeoCompleteEndpoint.as_view()),
    url(r'^agency/$', views.AgencyEndpoint.as_view(mode_list)),
    url(r'^agency/autocomplete', views.AgencyAutocomplete.as_view()),
    url(r'^recipients/autocomplete', views.RecipientAutocomplete.as_view()),
    url(r'^cfda/$', views.CfdaEndpoint.as_view(mode_list), name='cfda-list'),
    url(r'^cfda/(?P<program_number>[0-9]+\.[0-9]+)/', views.CfdaEndpoint.as_view(mode_detail), name='cfda-detail'),
]
