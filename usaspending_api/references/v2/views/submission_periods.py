import datetime

from rest_framework.views import APIView
from rest_framework.response import Response

from usaspending_api.submissions.models import DABSSubmissionWindowSchedule


class SubmissionPeriodsViewSet(APIView):

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/periods.md"

    def get(self, request):
        first_supported_period_date = datetime.datetime(2020, 4, 1)

        response_obj = {"available_periods": []}

        for record in DABSSubmissionWindowSchedule.objects.all().values():
            if first_supported_period_date <= record["submission_reveal_date"].replace(tzinfo=None) < datetime.datetime.now():
                response_obj["available_periods"].append({"fy": record["submission_fiscal_year"], "period": record["submission_fiscal_month"], "date": record["submission_reveal_date"]})

        return Response(response_obj)
