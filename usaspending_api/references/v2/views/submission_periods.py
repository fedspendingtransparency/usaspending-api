import datetime

from django.db.models import Max
from rest_framework.views import APIView
from rest_framework.response import Response

from usaspending_api.submissions.models import DABSSubmissionWindowSchedule

class SubmissionPeriodsViewSet(APIView):

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/periods.md"

    # this will return the max_submission_date given the fiscal year. used for the account download monthly picker
    def get(self, request):

        # set max date to low number
        max_submission_date = datetime.datetime(1776, 7, 4)

        # iterate through values for given fiscal year to find max submission date
        for record in DABSSubmissionWindowSchedule.objects.filter(submission_fiscal_year=2020).values():
            if max_submission_date.replace(tzinfo=None) < record['submission_reveal_date'].replace(tzinfo=None) < datetime.datetime.now().replace(tzinfo=None):
                max_submission_date = record['submission_reveal_date']

        return Response({"max_submission_date": max_submission_date})