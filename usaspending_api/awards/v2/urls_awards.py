from django.conf.urls import url

from usaspending_api.awards.v2.views.accounts import AwardAccountsViewSet
from usaspending_api.awards.v2.views.awards import AwardLastUpdatedViewSet, AwardRetrieveViewSet
from usaspending_api.awards.v2.views.funding_rollup import AwardFundingRollupViewSet

urlpatterns = [
    url(r"^accounts/$", AwardAccountsViewSet.as_view()),
    url(r"^funding_rollup/$", AwardFundingRollupViewSet.as_view()),
    url(r"^last_updated", AwardLastUpdatedViewSet.as_view()),
    url(r"^(?P<requested_award>[A-Za-z0-9_. -]+)/$", AwardRetrieveViewSet.as_view()),
]
