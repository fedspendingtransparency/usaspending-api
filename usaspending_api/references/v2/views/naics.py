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
        naics = NAICS.objects.annotate(text_len=Length("code")).filter(code__startswith=naics_code,
                                                                       text_len=len(naics_code) + 2)
        for naic in naics:
            if len(naic.code) < 6:
                result = OrderedDict()
                result["naics"] = naic.code
                result["naics_description"] = naic.description
                result["count"] = NAICS.objects.annotate(text_len=Length("code")).filter(code__startswith=naic.code,
                                                                                         text_len=6).count()
            else:
                result = OrderedDict()
                result["naics"] = naic.code
                result["naics_description"] = naic.description
                result["count"] = 0
            results.append(result)
        return results

    def _fetch_parent(self, naics_code) -> dict:
        parent = NAICS.objects.annotate(text_len=Length("code")).filter(code__startswith=naics_code[:2],
                                                                        text_len=2).first()
        parent2 = NAICS.objects.annotate(text_len=Length("code")).filter(code__startswith=naics_code[:4],
                                                                         text_len=4).first()
        results = OrderedDict()
        results["naics"] = parent.code
        results["description"] = parent.description
        results["count"] = NAICS.objects.annotate(text_len=Length("code")).filter(code__startswith=parent.code,
                                                                                  text_len=6).count()
        results2 = OrderedDict()
        results2["naics"] = parent2.code
        results2["description"] = parent2.description
        results2["count"] = NAICS.objects.annotate(text_len=Length("code")).filter(code__startswith=parent2.code,
                                                                                   text_len=6).count()
        results2["children"] = []
        results["children"] = [results2]
        return results

    def _business_logic(self, request_data: dict) -> list:
        naics_filter = {}
        code = request_data.get("code")
        description = request_data.get("description")

        if not code and not description:
            naics = NAICS.objects.annotate(text_len=Length("code")).filter(text_len=2)
            results = []
            for naic in naics:
                result = OrderedDict()
                result["naics"] = naic.code
                result["naics_description"] = naic.description
                result["count"] = NAICS.objects.annotate(text_len=Length("code")).filter(code__startswith=naic.code,
                                                                                         text_len=6).count()
                results.append(result)
            response_content = OrderedDict({"results": results})
            return response_content

        filtered = False
        if code:
            naics_filter.update({"code": request_data.get("code")})
        if description:
            filtered = True
            naics_filter.update({"description__icontains": request_data.get("description")})

        if filtered:
            naics = NAICS.objects.annotate(text_len=Length("code")).filter(**naics_filter, text_len=6)
        else:
            naics = NAICS.objects.filter(**naics_filter)
        results = []
        for naic in naics:
            if len(naic.code) < 6:
                result = OrderedDict()
                result["naics"] = naic.code
                result["naics_description"] = naic.description
                result["count"] = NAICS.objects.annotate(text_len=Length("code")).filter(code__startswith=naic.code,
                                                                                         text_len=6).count()
                result["children"] = self._fetch_children(naic.code)
            else:
                result = OrderedDict()
                if filtered:
                    parent = self._fetch_parent(naic.code)
                    result["naics"] = naic.code
                    result["naics_description"] = naic.description
                    result["count"] = 0
                    parent["children"][0]["children"].append(result)
                    result = parent
                else:
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
