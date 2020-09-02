from rest_framework.response import Response
from rest_framework.views import APIView
from requests import post
from time import sleep
from django.conf import settings
from usaspending_api.common.cache_decorator import cache_response

ALL_CFDAS = None


class CFDAViewSet(APIView):
    """
    Return an agency name and active fy.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/cfda/totals.md"

    @cache_response()
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
            remaining_tries = 30  # 30 attempts two seconds apart gives the max wait time for the API
            while not response:
                if remaining_tries == 0:
                    raise Exception("Failed to get successful response from Grants API!")
                sleep(2)
                response = self._request_from_grants_api()
                remaining_tries = remaining_tries - 1

            ALL_CFDAS = response.values()

    def _request_from_grants_api(self):
        return post(
            "https://www.grants.gov/grantsws/rest/opportunities/search/cfda/totals",
            headers={"Authorization": f"APIKEY={settings.GRANTS_API_KEY}"},
        ).json()["cfdas"]
