from django.conf.urls import url

from usaspending_api.awards import award_type as type_views
from usaspending_api.awards import recipient as recipient_views

# map reqest types to viewset method; replace this with a router
award_type = type_views.AwardTypeAwardSpendingViewSet.as_view({'get': 'list'})
recipient = recipient_views.RecipientAwardSpendingViewSet.as_view({'get': 'list'})

urlpatterns = [
    url(r'^award_type/', award_type, name='award-type-award-spending'),
    url(r'^recipient/', recipient, name='recipient-award-spending')
]