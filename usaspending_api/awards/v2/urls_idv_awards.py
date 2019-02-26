from django.conf.urls import url
from usaspending_api.awards.v2.views.idvs.amounts import IDVAmountsViewSet
from usaspending_api.awards.v2.views.idvs.awards import IDVAwardsViewSet
from usaspending_api.awards.v2.views.idvs.funding import IDVFundingViewSet

urlpatterns = [
    url(r'^amounts/(?P<requested_award>[A-Za-z0-9_. -]+)/$', IDVAmountsViewSet.as_view()),
    url(r'^awards/$', IDVAwardsViewSet.as_view()),
    url(r'^funding/$', IDVFundingViewSet.as_view()),
]
