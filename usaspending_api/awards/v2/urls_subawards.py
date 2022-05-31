from django.urls import re_path
from usaspending_api.awards.v2.views.subawards import SubawardsViewSet

urlpatterns = [re_path(r"^$", SubawardsViewSet.as_view())]
