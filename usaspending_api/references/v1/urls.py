from django.urls import re_path

from usaspending_api.references.v1 import views
from usaspending_api.common.views import RemovedEndpointView


urlpatterns = [
    re_path(r"^filter", views.FilterEndpoint.as_view()),
    re_path(r"^hash", views.HashEndpoint.as_view()),
    re_path(r"^locations/$", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})),
    re_path(r"^locations/geocomplete", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})),
    re_path(r"^agency/$", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})),
    re_path(r"^agency/autocomplete", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})),
    re_path(
        r"^recipients/$", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"}), name="recipient-list"
    ),
    re_path(
        r"^recipients/(?P<pk>[0-9]+)/$",
        RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"}),
        name="recipient-detail",
    ),
    re_path(r"^recipients/autocomplete", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})),
    re_path(r"^glossary/autocomplete", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})),
    re_path(r"^glossary/$", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})),
    re_path(r"^cfda/$", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"}), name="cfda-list"),
    re_path(
        r"^cfda/(?P<program_number>[0-9]+\.[0-9]+)/",
        RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"}),
        name="cfda-detail",
    ),
]
