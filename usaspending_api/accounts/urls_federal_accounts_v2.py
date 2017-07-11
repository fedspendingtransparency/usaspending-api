from django.conf.urls import url

from usaspending_api.accounts.views import federal_accounts_v2 as views

# bind ViewSets to URLs
object_class_federal_accounts = views.ObjectClassFederalAccountsViewSet.as_view()

urlpatterns = [
    url(r'^object_class/(?P<pk>[0-9]+)/$', object_class_federal_accounts)
]
