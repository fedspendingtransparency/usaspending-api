from django.conf.urls import url

from usaspending_api.references.v1 import views
from usaspending_api.common.views import RemovedEndpointView

mode_list = {"get": "list", "post": "list"}
mode_detail = {"get": "retrieve", "post": "retrieve"}


urlpatterns = [
    url(r"^filter", views.FilterEndpoint.as_view()),
    url(r"^hash", views.HashEndpoint.as_view()),
    url(r"^locations/$", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})),
    url(r"^locations/geocomplete", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})),
    url(r"^agency/$", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})),
    url(r"^agency/autocomplete", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})),
    url(r"^recipients/$", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"}), name="recipient-list"),
    url(
        r"^recipients/(?P<pk>[0-9]+)/$",
        RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"}),
        name="recipient-detail",
    ),
    url(r"^recipients/autocomplete", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})),
    url(r"^glossary/autocomplete", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})),
    url(r"^glossary/$", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})),
    url(r"^cfda/$", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"}), name="cfda-list"),
    url(
        r"^cfda/(?P<program_number>[0-9]+\.[0-9]+)/",
        RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"}),
        name="cfda-detail",
    ),
]
