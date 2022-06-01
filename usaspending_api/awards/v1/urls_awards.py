from django.urls import re_path

from usaspending_api.awards.v1 import views
from usaspending_api.common.views import RemovedEndpointView

# map request types to viewset method; replace this with a router
award_list = views.AwardListViewSet.as_view({"get": "list", "post": "list"})
award_detail = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})
award_total = views.AwardAggregateViewSet.as_view({"get": "list", "post": "list"})

urlpatterns = [
    re_path(r"^$", award_list, name="award-list"),
    re_path(r"(?P<pk>[0-9]+)/$", award_detail, name="award-detail"),
    re_path(r"^total/", award_total, name="award-total"),
]
