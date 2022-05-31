from django.urls import re_path
from usaspending_api.awards.v2.views.transactions import TransactionViewSet

urlpatterns = [re_path(r"^$", TransactionViewSet.as_view())]
