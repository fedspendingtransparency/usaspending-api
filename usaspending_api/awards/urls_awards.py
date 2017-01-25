from django.conf.urls import include, url

from usaspending_api.awards import views

award_summary_id_patterns = [
    url(r'^autocomplete/', views.AwardListSummaryAutocomplete.as_view())
]

# map reqest types to viewset method; replace this with a router
award_summary = views.AwardListSummaryViewSet.as_view({
    'get': 'list', 'post': 'list'})

urlpatterns = [
    url(r'^summary/', include(award_summary_id_patterns)),
    url(r'^summary/', award_summary)
]
