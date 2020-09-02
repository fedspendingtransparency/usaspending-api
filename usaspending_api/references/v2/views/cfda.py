from rest_framework.response import Response
from rest_framework.views import APIView
from requests import post
from time import sleep
from django.conf import settings

ALL_CFDAS = None


class CFDAViewSet(APIView):
    """
    Return an agency name and active fy.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/cfda/totals.md"

    def get(self, request, cfda=None):
        """
        Return the view's queryset.
        """
        self._populate_cfdas_if_needed()

        if cfda:
            results = [result for result in ALL_CFDAS if result["cfda"] == cfda]
        else:
            results = ALL_CFDAS

        response = {"results": results}

        return Response(response)

    def _populate_cfdas_if_needed(self):
        global ALL_CFDAS
        if not ALL_CFDAS:
            response = self._request_from_grants_api()

            #  grants API is brittle in practice, so if we don't get results retry at a polite rate
            while not response:
                sleep(2)
                response = self._request_from_grants_api()

            ALL_CFDAS = response.values()

    def _request_from_grants_api(self):
        return post(
            "https://www.grants.gov/grantsws/rest/opportunities/search/cfda/totals",
            headers={"Authorization": f"APIKEY={settings.GRANTS_API_KEY}"},
        ).json()["cfdas"]
