from rest_framework_extensions.cache.decorators import cache_response
from rest_framework.views import APIView
from rest_framework.response import Response

from usaspending_api.awards.models import Award


class AwardLastUpdatedViewSet(APIView):
    """Return all award spending by award type for a given fiscal year and agency id"""

    @cache_response()
    def get(self, request):
        """Return latest updated date for Awards"""
        response = {"last_updated": ''}

        # try/except is necessary since latest() raises an exception if no object is found
        try:
            latest_award = Award.objects.filter(update_date__isnull=False).latest('update_date')
            update_date = latest_award.update_date

            response['last_updated'] = update_date.strftime('%m/%d/%Y')
        except Award.DoesNotExist:
            pass

        return Response(response)
