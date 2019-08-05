from django.conf.urls import url
from usaspending_api.awards.v2.views.awards import AwardLastUpdatedViewSet, AwardRetrieveViewSet

urlpatterns = [
    url(r"^last_updated", AwardLastUpdatedViewSet.as_view()),
    url(r"^(?P<requested_award>[A-Za-z0-9_. -]+)/$", AwardRetrieveViewSet.as_view()),
]
