from django.conf.urls import url
from usaspending_api.awards.v2.views.idvs import IDVAmountsViewSet, IDVAwardsViewSet

urlpatterns = [
    url(r'^amounts/(?P<requested_award>[A-Za-z0-9_. -]+)/$', IDVAmountsViewSet.as_view()),
    url(r'^awards/$', IDVAwardsViewSet.as_view()),
]
