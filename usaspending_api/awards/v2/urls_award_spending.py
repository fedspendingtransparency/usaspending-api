from django.urls import re_path
from usaspending_api.awards.v2.views import award_spending as views


# map request types to viewset method; replace this with a router
recipient = views.RecipientAwardSpendingViewSet.as_view({"get": "list"})


urlpatterns = [re_path(r"^recipient/", recipient, name="recipient-award-spending")]
