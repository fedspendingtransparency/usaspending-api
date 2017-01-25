from django.conf.urls import url

from usaspending_api.awards import views

# map reqest types to viewset method; replace this with a router
award = views.AwardViewSet.as_view({
    'get': 'list', 'post': 'list'})

urlpatterns = [
    url(r'^$', award),
    url(r'^autocomplete/', views.AwardAutocomplete.as_view()),
]
