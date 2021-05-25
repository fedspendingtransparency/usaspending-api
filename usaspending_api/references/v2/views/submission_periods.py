import datetime

from rest_framework.views import APIView
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule


class SubmissionPeriodsViewSet(APIView):
    """
    This route sends a request to the backend to retrieve all relevant data regarding submission periods.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/submission_periods.md"

    @staticmethod
    def get_closed_submission_windows():

        # filter out records with submission window date after now
        subs = (
            DABSSubmissionWindowSchedule.objects.filter(submission_reveal_date__lte=datetime.datetime.now())
            .order_by("-submission_fiscal_year", "-submission_fiscal_month", "is_quarter")
            .values()
        )

        # remove id from the records
        for sub in subs:
            del sub["id"]

        return {"available_periods": subs}

    def get(self, request):

        models = [{"name": "use_cache", "key": "use_cache", "type": "boolean", "default": False}]
        validated_payload = TinyShield(models).block(request.GET)

        if validated_payload["use_cache"]:
            formatted_results = CachedSubmissionPeriodsViewSet.as_view()(request=request._request).data
        else:
            formatted_results = self.get_closed_submission_windows()

        return Response(formatted_results)


class CachedSubmissionPeriodsViewSet(APIView):
    """
    This ViewSet is only used internally to provided a cached version of the SubmissionPeriodsViewSet
    """

    @cache_response()
    def get(self, request):
        formatted_results = SubmissionPeriodsViewSet.get_closed_submission_windows()
        return Response(formatted_results)
