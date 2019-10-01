import logging
from collections import OrderedDict

from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView
from django.db.models.functions import Length

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.references.models import NAICS

logger = logging.getLogger("console")

class NAICSViewSet(APIView):

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/"

    def _parse_and_validate_request(self, requested_naics: str, request_data) -> dict:
        naics_filter = request_data.get("filter")
        return {"code": requested_naics,
                "description": naics_filter}

    def _fetch_children(self, naics_code) -> list:
        results = []
        naics = NAICS.objects.annotate(text_len=Length("code")).filter(code__startswith=naics_code, text_len=len(naics_code) + 2).exclude(code=naics_code)
        for naic in naics:
            if len(naic.code) < 6:
                result = OrderedDict()
                result["naics"] = naic.code
                result["naics_description"] = naic.description
                result["count"] = NAICS.objects.annotate(text_len=Length("code")).filter(code__startswith=naics_code, text_len=6).count()
                result["children"] = self._fetch_children(naic.code)
            else:
                result = OrderedDict()
                result["naics"] = naic.code
                result["naics_description"] = naic.description
                result["count"] = 0
            results.append(result)
        return results

    def _fetch_parents(self, naics_code) -> list:
        if len(naics_code) == 6:
            codes = [naics_code[:2], naics_code[:4]]
        elif len(naics_code = 4):
            codes = [naics_code[:2]]
        parents = 
        return []

    def _business_logic(self, request_data: dict) -> list:
        naics_filter = {}
        if request_data.get("code"):
            naics_filter.update({"code": request_data.get("code")})
        if request_data.get("description"):
            naics_filter.update({"description__icontains": request_data.get("description")})
        naics = NAICS.objects.filter(**naics_filter)
        results = []
        for naic in naics:
            if len(naic.code) < 6:
                result = OrderedDict()
                result["naics"] = naic.code
                result["naics_description"] = naic.description
                result["count"] = NAICS.objects.annotate(text_len=Length("code")).filter(code__startswith=naic.code, text_len=6).count()
                result["children"] = self._fetch_children(naic.code)
            else:
                result = OrderedDict()
                result["naics"] = naic.code
                result["naics_description"] = naic.description
                result["count"] = 0
            results.append(result)
        response_content = OrderedDict({"results": results})
        return response_content

    @cache_response()
    def get(self, request: Request, requested_naics: str = None) -> Response:
        request_data = self._parse_and_validate_request(requested_naics, request.GET)
        results = self._business_logic(request_data)
        return Response(results)
