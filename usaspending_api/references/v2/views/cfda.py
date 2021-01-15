import logging
from rest_framework.views import APIView
from requests import post
from rest_framework import status
from rest_framework.response import Response
from time import sleep
from django.conf import settings
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import NoDataFoundException, InternalServerError, ServiceUnavailable

CFDA_DICTIONARY = None
logger = logging.getLogger("console")


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
                raise NoDataFoundException(f"I can't find CFDA code '{cfda}' in my list; please check that is this the correct code.")

            try:
                response = {
                    "cfda": result["cfda"],
                    "posted": result["posted"],
                    "closed": result["closed"],
                    "archived": result["archived"],
                    "forecasted": result["forecasted"],
                }
            except KeyError:
                logger.exception("")
                raise InternalServerError("I can't process the data I received about CFDAs.")

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

            CFDA_DICTIONARY = response

    def _request_from_grants_api(self):

        # from http.client import HTTPConnection
        # HTTPConnection.debuglevel = 1
        # logging.basicConfig()
        # logging.getLogger().setLevel(logging.DEBUG)
        # requests_log = logging.getLogger("requests.packages.urllib3")
        # requests_log.setLevel(logging.DEBUG)
        # requests_log.propagate = True
        
        grants_response = post(
            "https://www.grants.gov/grantsws/rest/opportunities/search/cfda/totals",
            headers={"Authorization": f"APIKEY={settings.GRANTS_API_KEY}"},
        )
        if grants_response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE:
            raise ServiceUnavailable("Could not get list of CFDAs from the grants website.")
        
        return grants_response.json()["cfdas"]
