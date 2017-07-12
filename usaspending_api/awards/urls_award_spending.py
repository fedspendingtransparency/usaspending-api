from django.conf.urls import url

from usaspending_api.awards.views_v2 import award_spending as views

# map reqest types to viewset method; replace this with a router
award_type = views.AwardTypeAwardSpendingViewSet.as_view({'get': 'list'})
recipient = views.RecipientAwardSpendingViewSet.as_view({'get': 'list'})

urlpatterns = [
    url(r'^award_type/', award_type, name='award-type-award-spending'),
    url(r'^recipient/', recipient, name='recipient-award-spending')
]
