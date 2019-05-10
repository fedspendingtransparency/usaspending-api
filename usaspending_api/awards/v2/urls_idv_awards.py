from django.conf.urls import url
from usaspending_api.awards.v2.views.idvs.accounts import IDVAccountsViewSet
from usaspending_api.awards.v2.views.idvs.amounts import IDVAmountsViewSet
from usaspending_api.awards.v2.views.idvs.awards import IDVAwardsViewSet
from usaspending_api.awards.v2.views.idvs.funding import IDVFundingViewSet
from usaspending_api.awards.v2.views.idvs.funding_rollup import IDVFundingRollupViewSet


urlpatterns = [
    url(r"^amounts/(?P<requested_award>[A-Za-z0-9_. -]+)/$", IDVAmountsViewSet.as_view()),
    url(r"^awards/$", IDVAwardsViewSet.as_view()),
    url(r"^funding/$", IDVFundingViewSet.as_view()),
    url(r"^funding_rollup/$", IDVFundingRollupViewSet.as_view()),
    url(r"^accounts/$", IDVAccountsViewSet.as_view()),
]
