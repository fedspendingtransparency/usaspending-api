from django.conf.urls import url

from usaspending_api.awards import views

# map reqest types to viewset method; replace this with a router
award_type_award_spending = views.AwardTypeAwardSpendingViewSet.as_view({'get': 'retrieve'})

urlpatterns = [
    url(r'^award_type/', award_type_award_spending, name='award-type-award-spending')
    # url(r'^recipient/')
]
