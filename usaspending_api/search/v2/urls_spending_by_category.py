from django.conf.urls import url

from usaspending_api.search.v2.views.spending_by_category_views.spending_by_agency_types import AwardingAgencyViewSet
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_agency_types import AwardingSubagencyViewSet


urlpatterns = [
    url(r"^awarding_agency", AwardingAgencyViewSet.as_view()),
    url(r"^awarding_subagency", AwardingSubagencyViewSet.as_view()),
]
