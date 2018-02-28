from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.views import APIDocumentationView
from rest_framework.response import Response
from django.db.models import Max

from usaspending_api.awards.models import Award


class AwardLastUpdatedViewSet(APIDocumentationView):
    """Return all award spending by award type for a given fiscal year and agency id.
    GITHUB DOCUMENTATION: /awards/last_updated.md
    """

    @cache_response()
    def get(self, request):
        """Return latest updated date for Awards"""

        max_update_date = Award.objects.all().aggregate(Max('update_date'))['update_date__max']
        response = {
            "last_updated": max_update_date.strftime('%m/%d/%Y')
        }

        return Response(response)
