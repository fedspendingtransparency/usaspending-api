from django.conf.urls import url
from usaspending_api.accounts import views

urlpatterns = [
    url(r'^$', views.AppropriationAccountBalancesList.as_view()),
    url(r'^tas/', views.TreasuryAppropriationAccountList.as_view())
]
