from django.conf.urls import url
from usaspending_api.idvs.v2.views.accounts import IDVAccountsViewSet
from usaspending_api.idvs.v2.views.activity import IDVActivityViewSet
from usaspending_api.idvs.v2.views.amounts import IDVAmountsViewSet
from usaspending_api.idvs.v2.views.awards import IDVAwardsViewSet
from usaspending_api.idvs.v2.views.funding import IDVFundingViewSet
from usaspending_api.idvs.v2.views.funding_rollup import IDVFundingRollupViewSet
from usaspending_api.idvs.v2.views.count.federal_account import IDVFederalAccountCountViewSet


urlpatterns = [
    url(r"^accounts/$", IDVAccountsViewSet.as_view()),
    url(r"^activity/$", IDVActivityViewSet.as_view()),
    url(r"^amounts/(?P<requested_award>[0-9]+)/$", IDVAmountsViewSet.as_view()),
    url("^amounts/(?P<requested_award>(CONT|ASST)_(AWD|IDV|NON|AGG)_.+)/$", IDVAmountsViewSet.as_view()),
    url(r"^awards/$", IDVAwardsViewSet.as_view()),
    url(r"^funding/$", IDVFundingViewSet.as_view()),
    url(r"^funding_rollup/$", IDVFundingRollupViewSet.as_view()),
    url(r"^count/federal_account/(?P<requested_award>[0-9]+)/$", IDVFederalAccountCountViewSet.as_view()),
    url(
        "^count/federal_account/(?P<requested_award>(CONT|ASST)_(AWD|IDV|NON|AGG)_.+)/$",
        IDVFederalAccountCountViewSet.as_view(),
    ),
]
