from rest_framework.response import Response
from rest_framework.views import APIView
from requests import post
from time import sleep


class CFDAViewSet(APIView):
    """
    Return an agency name and active fy.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/agency/id.md"

    def get(self, request, cfda=None):
        """
        Return the view's queryset.
        """
        response = self._request_from_grants_api()

        #  grants API is brittle in practice, so if we don't get results retry at a polite rate
        while not response:
            sleep(2)
            response = self._request_from_grants_api()

        results = response.values()

        if cfda:
            results = [result for result in results if result["cfda"] == cfda]

        response = {"results": results}

        return Response(response)

    def _request_from_grants_api(self):
        # DO NOT PUT THIS IN THE CODEBASE!!!!!
        VERYMUCHTEMPORARYKEY = ""
        return post(
            "https://www.grants.gov/grantsws/rest/opportunities/search/cfda/totals",
            headers={"Authorization": f"APIKEY={VERYMUCHTEMPORARYKEY}"},
        ).json()["cfdas"]
