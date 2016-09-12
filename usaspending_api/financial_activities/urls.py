from django.conf.urls import url
from usaspending_api.financial_activities import views

urlpatterns = [
    url(r'^', views.FinancialAccountsByProgramActivityObjectClassList.as_view()),
]
