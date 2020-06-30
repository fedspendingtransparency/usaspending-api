
from rest_framework.views import APIView


class PeriodsViewSet(APIView):

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/periods.md"

    def get(self, request):
        print(request)

        return "done"
        #return Response({"codes": rows})
