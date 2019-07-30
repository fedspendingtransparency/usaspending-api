from django.conf.urls import url

from usaspending_api.awards.v1 import views

# map reqest types to viewset method; replace this with a router
transaction_list = views.TransactionListViewset.as_view({"get": "list", "post": "list"})
transaction_detail = views.TransactionRetrieveViewset.as_view({"get": "retrieve", "post": "retrieve"})
transaction_total = views.TransactionAggregateViewSet.as_view({"get": "list", "post": "list"})

urlpatterns = [
    url(r"^$", transaction_list, name="transaction-list"),
    url(r"(?P<pk>[0-9]+)/$", transaction_detail, name="transaction-detail"),
    url(r"^total/", transaction_total, name="transaction-total"),
]
