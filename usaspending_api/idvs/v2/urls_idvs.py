from django.conf.urls import url
from usaspending_api.idvs.v2.views.accounts import IDVAccountsViewSet
from usaspending_api.idvs.v2.views.activity import IDVActivityViewSet
from usaspending_api.idvs.v2.views.amounts import IDVAmountsViewSet
from usaspending_api.idvs.v2.views.awards import IDVAwardsViewSet
from usaspending_api.idvs.v2.views.funding import IDVFundingViewSet
from usaspending_api.idvs.v2.views.funding_rollup import IDVFundingRollupViewSet


urlpatterns = [
    url(r"^accounts/$", IDVAccountsViewSet.as_view()),
    url(r"^activity/$", IDVActivityViewSet.as_view()),
    url(r"^amounts/(?P<requested_award>[A-Za-z0-9_. -]+)/$", IDVAmountsViewSet.as_view()),
    url(r"^awards/$", IDVAwardsViewSet.as_view()),
    url(r"^funding/$", IDVFundingViewSet.as_view()),
    url(r"^funding_rollup/$", IDVFundingRollupViewSet.as_view()),
]
