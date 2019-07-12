from django.conf.urls import url
from usaspending_api.awards.v2.views.subawards import SubawardsViewSet

urlpatterns = [url(r"^$", SubawardsViewSet.as_view())]
