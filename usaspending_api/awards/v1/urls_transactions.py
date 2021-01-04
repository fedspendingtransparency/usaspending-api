from django.conf.urls import url

from usaspending_api.common.views import RemovedEndpointView

# map request types to viewset method; replace this with a router
transaction_list = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})
transaction_detail = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})
transaction_total = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})

urlpatterns = [
    url(r"^$", transaction_list, name="transaction-list"),
    url(r"(?P<pk>[0-9]+)/$", transaction_detail, name="transaction-detail"),
    url(r"^total/", transaction_total, name="transaction-total"),
]
