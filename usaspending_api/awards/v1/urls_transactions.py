from django.urls import re_path

from usaspending_api.common.views import RemovedEndpointView

# map request types to viewset method; replace this with a router
transaction_list = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})
transaction_detail = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})
transaction_total = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})

urlpatterns = [
    re_path(r"^$", transaction_list, name="transaction-list"),
    re_path(r"(?P<pk>[0-9]+)/$", transaction_detail, name="transaction-detail"),
    re_path(r"^total/", transaction_total, name="transaction-total"),
]
