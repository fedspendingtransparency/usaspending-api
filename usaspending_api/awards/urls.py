from django.conf.urls import include, url

from usaspending_api.awards import views

award_id_patterns = [
    url(r'^total/', views.AwardListAggregate.as_view())
]

award_summary_id_patterns = [
    url(r'^autocomplete/', views.AwardListSummaryAutocomplete.as_view())
]

# map reqest types to viewset method; replace this with a router
award = views.AwardListViewSet.as_view(
    {'get': 'list', 'post': 'list'})
award_summary = views.AwardListSummaryViewSet.as_view({
    'get': 'list', 'post': 'list'})

urlpatterns = [
    url(r'^$', award),
    url(r'^uri/(?P<uri>\w+)', award),
    url(r'^fain/(?P<fain>\w+)', award),
    url(r'^piid/(?P<piid>\w+)', award),
    url(r'^summary/', include(award_summary_id_patterns)),
    url(r'^summary/', award_summary),
    url(r'^total/', include(award_id_patterns))
]
