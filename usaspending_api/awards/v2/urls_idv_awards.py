from django.conf.urls import url
from usaspending_api.awards.v2.views.idvs.amounts import IDVAmountsViewSet
from usaspending_api.awards.v2.views.idvs.awards import IDVAwardsViewSet
from usaspending_api.awards.v2.views.idvs.funding import IDVFundingViewSet
from usaspending_api.awards.v2.views.idvs.funding_treemap import IDVFundingRollupViewSet  # , IDVFundingTreemapViewSet

urlpatterns = [
    url(r'^amounts/(?P<requested_award>[A-Za-z0-9_. -]+)/$', IDVAmountsViewSet.as_view()),
    url(r'^awards/$', IDVAwardsViewSet.as_view()),
    url(r'^funding/$', IDVFundingViewSet.as_view()),
    url(r'^funding-rollup/$', IDVFundingRollupViewSet.as_view()),
    # This is a stub for the tree map endpoint fo DEV-2237
    # url(r'^accounts/$', IDVFundingTreemapViewSet.as_view()),
]
