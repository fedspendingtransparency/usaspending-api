from rest_framework.views import APIView
from requests import post
from rest_framework.response import Response
from time import sleep
from django.conf import settings
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import NoDataFoundException
import logging

logger = logging.getLogger("console")
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
                logger.error(f"Data from grants API not in expected format: {result}")
                raise NoDataFoundException(f"Data from grants API not in expected format: {result}")

        else:
            response = {"results": CFDA_DICTIONARY.values()}

        return Response(response)

    def _populate_cfdas_if_needed(self):
        global CFDA_DICTIONARY
        if not CFDA_DICTIONARY:
            response = post(
                "https://www.grants.gov/grantsws/rest/opportunities/search/cfda/totals",
                headers={"Authorization": f"APIKEY={settings.GRANTS_API_KEY}"},
            )
            if response.status_code != 200:
                logger.error(f"Status returned by grants API: {response.status_code}")
                raise NoDataFoundException(f"Status returned by grants API: {response.status_code}")
            response = response.json()
            if response.get("errorMsgs") and response["errorMsgs"] != []:
                logger.error(f"Error returned by grants API: {response['errorMsgs']}")
                raise NoDataFoundException(f"Error returned by grants API: {response['errorMsgs']}")

            CFDA_DICTIONARY = response
