from django.conf.urls import url

from usaspending_api.common.views import RemovedEndpointView

# map request types to viewset method; replace this with a router
subaward_list = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})
subaward_detail = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})
subaward_total = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})

urlpatterns = [
    url(r"^$", subaward_list, name="subaward-list"),
    url(r"(?P<pk>[0-9]+)/$", subaward_detail, name="subaward-detail"),
    url(r"^autocomplete/", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})),
    url(r"^total/", subaward_total, name="subaward-total"),
]
