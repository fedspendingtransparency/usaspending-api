from django.conf.urls import url

from usaspending_api.awards.v1 import views

# map reqest types to viewset method; replace this with a router
subaward_list = views.SubawardListViewSet.as_view({"get": "list", "post": "list"})
subaward_detail = views.SubawardRetrieveViewSet.as_view({"get": "retrieve", "post": "retrieve"})
subaward_total = views.SubawardAggregateViewSet.as_view({"get": "list", "post": "list"})

urlpatterns = [
    url(r"^$", subaward_list, name="subaward-list"),
    url(r"(?P<pk>[0-9]+)/$", subaward_detail, name="subaward-detail"),
    url(r"^autocomplete/", views.SubawardAutocomplete.as_view()),
    url(r"^total/", subaward_total, name="subaward-total"),
]
