from rest_framework.response import Response
from rest_framework.views import APIView
from requests import post
from time import sleep
from django.conf import settings
from usaspending_api.common.cache_decorator import cache_response

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
                return Response(status=404)

            response = {
                "cfda": result["cfda"],
                "posted": result["posted"],
                "closed": result["closed"],
                "archived": result["archived"],
                "forecasted": result["forecasted"],
            }
        else:
            response = {"results": CFDA_DICTIONARY.values()}

        return Response(response)

    def _populate_cfdas_if_needed(self):
        global CFDA_DICTIONARY
        if not CFDA_DICTIONARY:
            response = self._request_from_grants_api()

            #  grants API is brittle in practice, so if we don't get results retry at a polite rate
            remaining_tries = 30  # 30 attempts two seconds apart gives the max wait time for the API
            while not response:
                if remaining_tries == 0:
                    raise Exception("Failed to get successful response from Grants API!")
                sleep(2)
                response = self._request_from_grants_api()
                remaining_tries = remaining_tries - 1

            if response.status_code == 200:
                CFDA_DICTIONARY = response
            elif response.status_code == 204:
                #no content
            elif response.status_code == 408:
                #timeout
            elif response.status_code == 503:
                #unavailable
            else:
                raise Exception("Failed to get Grants data; code {0}: {1}", response.status_code, response.reason)

    def _request_from_grants_api(self):
        return post(
            "https://www.grants.gov/grantsws/rest/opportunities/search/cfda/totals",
            headers={"Authorization": f"APIKEY={settings.GRANTS_API_KEY}"},
        ).json()["cfdas"]
