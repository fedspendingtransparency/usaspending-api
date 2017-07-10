from django.conf.urls import url

from usaspending_api.accounts.views import budget_authority as views

# bind ViewSets to URLs
budget_authorities_agency = views.BudgetAuthorityViewSet.as_view(
    {'get': 'list'})

urlpatterns = [
    url(r'^agencies/(?P<cgac>\w+)/$', budget_authorities_agency),
]
