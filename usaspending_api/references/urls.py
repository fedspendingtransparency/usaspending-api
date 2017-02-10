from django.conf.urls import url
from usaspending_api.references import views

urlpatterns = [
    url(r'^locations/$', views.LocationEndpoint.as_view()),
    url(r'^locations/geocomplete', views.LocationEndpoint.as_view(), {'geocomplete': True}),
    url(r'^agency/$', views.AgencyEndpoint.as_view({'get': 'list', 'post': 'list'})),
    url(r'^agency/autocomplete', views.AgencyAutocomplete.as_view()),
    url(r'^recipients/autocomplete', views.RecipientAutocomplete.as_view()),
]
