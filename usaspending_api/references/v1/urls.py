from django.conf.urls import url

from usaspending_api.references.v1 import views

mode_list = {"get": "list", "post": "list"}
mode_detail = {"get": "retrieve", "post": "retrieve"}


urlpatterns = [url(r"^filter", views.FilterEndpoint.as_view()), url(r"^hash", views.HashEndpoint.as_view())]
