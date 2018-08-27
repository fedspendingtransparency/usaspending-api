from django.conf.urls import url
from usaspending_api.awards.v2.views.awards import AwardLastUpdatedViewSet, AwardRetrieveViewSet

urlpatterns = [
    url(r'^last_updated', AwardLastUpdatedViewSet.as_view()),
    url(r'(?P<generated_unique_award_id>[A-Za-z0-9_. -]+)/$', AwardRetrieveViewSet.as_view()),
    ]
