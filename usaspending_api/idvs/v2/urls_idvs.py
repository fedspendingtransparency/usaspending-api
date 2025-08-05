from django.urls import re_path
from usaspending_api.idvs.v2.views.accounts import IDVAccountsViewSet
from usaspending_api.idvs.v2.views.activity import IDVActivityViewSet
from usaspending_api.idvs.v2.views.amounts import IDVAmountsViewSet
from usaspending_api.idvs.v2.views.awards import IDVAwardsViewSet
from usaspending_api.idvs.v2.views.funding import IDVFundingViewSet
from usaspending_api.idvs.v2.views.funding_rollup import IDVFundingRollupViewSet
from usaspending_api.idvs.v2.views.count.federal_account import IDVFederalAccountCountViewSet


urlpatterns = [
    re_path(r"^accounts/$", IDVAccountsViewSet.as_view()),
    re_path(r"^activity/$", IDVActivityViewSet.as_view()),
    re_path("^amounts/(?P<requested_award>(CONT_IDV_.+)|([0-9]+))/$", IDVAmountsViewSet.as_view()),
    re_path(r"^awards/$", IDVAwardsViewSet.as_view()),
    re_path(r"^funding/$", IDVFundingViewSet.as_view()),
    re_path(r"^funding_rollup/$", IDVFundingRollupViewSet.as_view()),
    re_path(
        "^count/federal_account/(?P<requested_award>(CONT_IDV_.+)|([0-9]+))/$", IDVFederalAccountCountViewSet.as_view()
    ),
]
