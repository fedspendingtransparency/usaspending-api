from django.urls import re_path

from usaspending_api.submissions import views

# bind ViewSets to URLs
submission_list = views.SubmissionAttributesViewSet.as_view({"get": "list", "post": "list"})

urlpatterns = [re_path(r"^$", submission_list)]
