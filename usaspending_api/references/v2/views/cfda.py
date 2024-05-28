from rest_framework.views import APIView
from requests import post
from rest_framework.response import Response
from django.conf import settings
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import NoDataFoundException
import logging

logger = logging.getLogger(__name__)
CFDA_DICTIONARY = None


class CFDAViewSet(APIView):
    """
    Returns opportunity totals for a CFDA or all opportunity totals.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/cfda/totals.md"

    @cache_response()
    def get(self, request, cfda=None):
        """
        Return the CFDA's opportunity totals from grants.gov, or totals for all current CFDAs.
        """
        global CFDA_DICTIONARY
        if not CFDA_DICTIONARY:
            CFDA_DICTIONARY = self.get_cfdas()

        if cfda:
            result = CFDA_DICTIONARY.get(cfda)
            if not result:
                raise NoDataFoundException(f"No grant records found for '{cfda}'.")

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

    def get_cfdas(self):
        cdfas = {}
        remaining_tries = 10

        while not cdfas and remaining_tries:
            remaining_tries -= 1
            response = post(
                settings.GRANTS_URL,
                headers={"Authorization": f"APIKEY={settings.GRANTS_API_KEY}"},
            )
            if response.status_code != 200:
                logger.error(f"Error returned by grants API: {response}")
                raise NoDataFoundException("Grant data is not currently available.")
            response = response.json()
            if response.get("errorMsgs") and response["errorMsgs"] != []:
                logger.error(f"Error returned by grants API: {response['errorMsgs']}")
                raise NoDataFoundException("Grant data is not currently available.")

            if response.get("cfdas"):
                return self.check_response(response)

    def check_response(self, response):
        cfdas = response.get("cfdas")
        if not cfdas:
            logger.error(f"Response from grants API missing 'cfdas' key, full response: {response}")
            raise NoDataFoundException("Grant data is not currently available.")
        else:
            key_check = {"cfda", "posted", "closed", "archived", "forecasted"}
            for cfda in cfdas.values():
                if key_check - set(cfda):
                    logger.error(f"Data from grants API not in expected format: {cfdas}")
                    raise NoDataFoundException("Grant data is not currently available.")
        return cfdas
