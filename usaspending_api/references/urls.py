from django.conf.urls import url, include
from usaspending_api.references import views
from rest_framework.routers import DefaultRouter

guide_router = DefaultRouter()
guide_router.register('guide', views.GuideViewSet)

mode_list = {'get': 'list', 'post': 'list'}
mode_detail = {'get': 'retrieve', 'post': 'retrieve'}


urlpatterns = [
    url(r'^filter', views.FilterEndpoint.as_view()),
    url(r'^hash', views.HashEndpoint.as_view()),
    url(r'^locations/$', views.LocationEndpoint.as_view(mode_list)),
    url(r'^locations/geocomplete', views.LocationGeoCompleteEndpoint.as_view()),
    url(r'^agency/$', views.AgencyEndpoint.as_view(mode_list)),
    url(r'^agency/(?P<pk>[0-9]+)/$', views.AgencyEndpoint.as_view(mode_detail), name='agency-detail'),
    url(r'^agency/autocomplete', views.AgencyAutocomplete.as_view()),
    url(r'^recipients/$', views.RecipientViewSet.as_view(mode_list), name='recipient-list'),
    url(r'^recipients/(?P<pk>[0-9]+)/$', views.RecipientViewSet.as_view(mode_detail), name='recipient-detail'),
    url(r'^recipients/autocomplete', views.RecipientAutocomplete.as_view()),
    url(r'^guide/autocomplete', views.GuideAutocomplete.as_view()),
    url(r'', include(guide_router.urls)),
    url(r'^cfda/$', views.CfdaEndpoint.as_view(mode_list), name='cfda-list'),
    url(r'^cfda/(?P<program_number>[0-9]+\.[0-9]+)/', views.CfdaEndpoint.as_view(mode_detail), name='cfda-detail'),
]
