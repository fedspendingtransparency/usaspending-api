from django.conf.urls import url

from usaspending_api.awards import views

# map reqest types to viewset method; replace this with a router
subagency_award_spending = views.SubagencyAwardSpending.as_view({'post': 'list'})

urlpatterns = [
    url(r'^subagency/', subagency_award_spending, name='subagency-award-spending')
]
