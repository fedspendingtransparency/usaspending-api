from django.conf.urls import url

from usaspending_api.submissions import views

# bind ViewSets to URLs
submission_list = views.SubmissionAttributesViewSet.as_view({"get": "list", "post": "list"})

urlpatterns = [url(r"^$", submission_list)]
