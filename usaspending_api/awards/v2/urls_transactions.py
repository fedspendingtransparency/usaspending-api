from django.conf.urls import url
from usaspending_api.awards.v2.views.transactions import TransactionViewSet

urlpatterns = [url(r"^$", TransactionViewSet.as_view())]
