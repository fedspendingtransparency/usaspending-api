from django.urls import re_path

from usaspending_api.common.views import RemovedEndpointView

# map request types to viewset method; replace this with a router
subaward_list = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})
subaward_detail = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})
subaward_total = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})

urlpatterns = [
    re_path(r"^$", subaward_list, name="subaward-list"),
    re_path(r"(?P<pk>[0-9]+)/$", subaward_detail, name="subaward-detail"),
    re_path(r"^autocomplete/", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})),
    re_path(r"^total/", subaward_total, name="subaward-total"),
]
