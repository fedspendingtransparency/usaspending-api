from django.conf.urls import url
from usaspending_api.awards.v2.views.awards import AwardLastUpdatedViewSet

urlpatterns = [
    url(r'^last_updated', AwardLastUpdatedViewSet.as_view())
    ]
