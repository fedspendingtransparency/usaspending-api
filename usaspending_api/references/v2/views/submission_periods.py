import datetime

from rest_framework.views import APIView
from rest_framework.response import Response

from usaspending_api.submissions.models import DABSSubmissionWindowSchedule


class SubmissionPeriodsViewSet(APIView):

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/periods.md"

    # this will return the max_submission_date given the fiscal year. used for the account download monthly picker
    def get(self, request):

        # set max date to low number
        response_object = {"available_periods": []}

        # filter out records with submission window date after now
        subs = DABSSubmissionWindowSchedule.objects.filter(submission_reveal_date__lte=datetime.datetime.now()).values()

        # remove id from the records
        for sub in subs:
            del sub["id"]

        response_object["available_periods"] = subs

        return Response({"available_periods": subs})
