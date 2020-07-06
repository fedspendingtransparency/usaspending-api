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
        response_object = {"available_periods": []}

        max_submission_date = datetime.datetime(1776, 7, 4)
        max_period = 0
        previous_record = None

        for record in DABSSubmissionWindowSchedule.objects.filter().values():

            if previous_record:
                if previous_record['submission_fiscal_year'] != record['submission_fiscal_year']:
                    response_object["available_periods"].append(
                        {'fy': previous_record['submission_fiscal_year'], 'period': max_period})

            if max_submission_date.replace(tzinfo=None) < record['submission_reveal_date'].replace(tzinfo=None) < datetime.datetime.now().replace(tzinfo=None):
                max_submission_date = record['submission_reveal_date']
                max_period = record['submission_fiscal_month']

            previous_record = record

        response_object["available_periods"].append({'fy': record['submission_fiscal_year'], 'period': max_period})


        return Response(response_object)