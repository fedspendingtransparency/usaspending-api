from django.conf.urls import include, url
from usaspending_api.awards import views

award_id_patterns = [
    url(r'^$', views.AwardList.as_view()),
    url(r'^uri/(?P<uri>\w+)', views.AwardList.as_view()),
    url(r'^fain/(?P<fain>\w+)', views.AwardList.as_view()),
    url(r'^piid/(?P<piid>\w+)', views.AwardList.as_view()),
    url(r'^total/', views.AwardListAggregate.as_view())
]

award_summary_id_patterns = [
    url(r'^$', views.AwardListSummary.as_view()),
    url(r'^uri/(?P<uri>.+)', views.AwardListSummary.as_view()),
    url(r'^fain/(?P<fain>.+)', views.AwardListSummary.as_view()),
    url(r'^piid/(?P<piid>.+)', views.AwardListSummary.as_view()),
    url(r'^autocomplete/', views.AwardListSummaryAutocomplete.as_view())

]

urlpatterns = [
    url(r'', include(award_id_patterns)),
    url(r'^summary/', include(award_summary_id_patterns))
]
