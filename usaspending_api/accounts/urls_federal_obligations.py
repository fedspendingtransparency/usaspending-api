from django.conf.urls import url

from usaspending_api.accounts.views import federal_obligations as views

# bind ViewSets to URLs
federal_list = views.FederalAccountByObligationViewSet.as_view({"get": "list", "post": "list"})

urlpatterns = [url(r"^$", federal_list)]
