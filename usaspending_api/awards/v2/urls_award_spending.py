from django.conf.urls import url

from usaspending_api.awards.v2.views import award_spending as views

# map request types to viewset method; replace this with a router
award_category = views.AwardTypeAwardSpendingViewSet.as_view({"get": "list"})
recipient = views.RecipientAwardSpendingViewSet.as_view({"get": "list"})

urlpatterns = [
    # url(r'^award_category/', award_category, name='award-category-award-spending'),
    url(r"^recipient/", recipient, name="recipient-award-spending")
]
