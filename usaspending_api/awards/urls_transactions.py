from django.conf.urls import url

from usaspending_api.awards import views

# map reqest types to viewset method; replace this with a router
transaction = views.TransactionViewset.as_view(
    {'get': 'list', 'post': 'list'})
transaction_total = views.TransactionAggregateViewSet.as_view({
    'get': 'list', 'post': 'list'})

urlpatterns = [
    url(r'^$', transaction),
    url(r'^total/', transaction_total)
]
