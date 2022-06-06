from django.urls import re_path

from usaspending_api.accounts.views import federal_obligations as views

# bind ViewSets to URLs
federal_list = views.FederalAccountByObligationViewSet.as_view({"get": "list", "post": "list"})

urlpatterns = [re_path(r"^$", federal_list)]
