from django.conf.urls import url
from usaspending_api.awards import views

urlpatterns = [
    url(r'^', views.AwardList.as_view()),
]
