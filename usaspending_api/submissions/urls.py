from django.conf.urls import url
from usaspending_api.submissions import views

urlpatterns = [
    url(r'^', views.SubmissionAttributesList.as_view()),
]
