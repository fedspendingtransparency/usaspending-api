from django.conf.urls import url

from usaspending_api.awards.views_v2 import industry_codes as views

industry_codes = views.TransactionContractViewSet.as_view({'get': 'list'})

urlpatterns = [
    url(r'^$', industry_codes, name='industry-codes')
]
