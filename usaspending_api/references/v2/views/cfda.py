from rest_framework.views import APIView
from requests import post
from rest_framework.response import Response
from time import sleep
from django.conf import settings
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import NoDataFoundException

CFDA_DICTIONARY = None


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
            result = CFDA_DICTIONARY.get(cfda)

            if not result:
                raise NoDataFoundException(f"No grant records found for '{cfda}'.")

            try:
                response = {
                    "cfda": result["cfda"],
                    "posted": result["posted"],
                    "closed": result["closed"],
                    "archived": result["archived"],
                    "forecasted": result["forecasted"],
                }
            except KeyError:
                raise NoDataFoundException(f"Data from grants API not in expected format: {result}")

        else:
            response = {"results": CFDA_DICTIONARY.values()}

        return Response(response)

    def _populate_cfdas_if_needed(self):
        global CFDA_DICTIONARY
        if not CFDA_DICTIONARY:
            response = self._request_from_grants_api()

            #  grants API is brittle in practice, so if we don't get results retry at a polite rate
            # update 1/27/21 now that we're checking status & results, is there any value in this loop?
            remaining_tries = 30  # 30 attempts two seconds apart gives the max wait time for the API
            while not response:
                if remaining_tries == 0:
                    raise Exception("Failed to get successful response from Grants API!")
                sleep(2)
                response = self._request_from_grants_api()
                remaining_tries = remaining_tries - 1

            CFDA_DICTIONARY = response

    def _request_from_grants_api(self):
        cfda_response = post(
            "https://www.grants.gov/grantsws/rest/opportunities/search/cfda/totals",
            headers={"Authorization": f"APIKEY={settings.GRANTS_API_KEY}"},
        )
        if cfda_response.status_code != 200:
            raise NoDataFoundException(f"Status returned by grants API: {cfda_response.status_code}")
        if cfda_response.json()["errorMsgs"] != []:
            raise NoDataFoundException(f"Error returned by grants API: {cfda_response.json()['errorMsgs']}")

        return cfda_response.json()["cfdas"]
