import datetime

from rest_framework.views import APIView
from rest_framework.response import Response

from usaspending_api.submissions.models import DABSSubmissionWindowSchedule


class SubmissionPeriodsViewSet(APIView):
    """
    This route sends a request to the backend to retrieve all relevant data regarding submission periods.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/submission_periods.md"

    def get(self, request):

        # filter out records with submission window date after now
        subs = (
            DABSSubmissionWindowSchedule.objects.filter(submission_reveal_date__lte=datetime.datetime.now())
            .order_by("-submission_fiscal_year", "-submission_fiscal_month", "is_quarter")
            .values()
        )

        # remove id from the records
        for sub in subs:
            del sub["id"]

        return Response({"available_periods": subs})
