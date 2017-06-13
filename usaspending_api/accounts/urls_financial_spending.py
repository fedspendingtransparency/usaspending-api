from django.conf.urls import url

from usaspending_api.accounts.views import financial_spending as views

# bind ViewSets to URLs
object_class_financial_spending = views.ObjectClassFinancialSpendingViewSet.as_view(
    {'get': 'list'})

urlpatterns = [
    url(r'^object_class/$', object_class_financial_spending)
]
