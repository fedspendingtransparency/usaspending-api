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
    """
    Return a list of NAICS or a filtered list of NAICS
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/naics.md"

    def _parse_and_validate_request(self, requested_naics: str, request_data) -> dict:
        naics_filter = request_data.get("filter")
        data = {"code": requested_naics, "description": naics_filter}
        models = [
            {"key": "code", "name": "code", "type": "integer", "allow_nulls": True, "optional": True},
            {
                "key": "description",
                "name": "description",
                "type": "text",
                "text_type": "search",
                "default": None,
                "optional": True,
                "allow_nulls": True,
            },
        ]
        validated = TinyShield(models).block(data)
        return validated

    def _fetch_children(self, naics_code, naics_filter=None) -> list:
        results = []
        if naics_filter:
            naics = NAICS.objects.annotate(text_len=Length("code")).filter(
                **naics_filter, code__startswith=naics_code, text_len=len(naics_code) + 2
            )
        else:
            naics = NAICS.objects.annotate(text_len=Length("code")).filter(
                code__startswith=naics_code, text_len=len(naics_code) + 2
            )
        for naic in naics:
            if len(naic.code) < 6:
                result = OrderedDict()
                result["naics"] = naic.code
                result["naics_description"] = naic.description
                result["count"] = (
                    NAICS.objects.annotate(text_len=Length("code"))
                    .filter(code__startswith=naic.code, text_len=6)
                    .count()
                )
            else:
                result = OrderedDict()
                result["naics"] = naic.code
                result["naics_description"] = naic.description
                result["count"] = 0
            results.append(result)
        return results

    def _filter_search(self, naics_filter: dict) -> dict:
        tier1_codes = set([])
        tier2_codes = set([])
        tier3_codes = set([])

        tier3_naics = NAICS.objects.annotate(text_len=Length("code")).filter(**naics_filter, text_len=6)
        for naic in tier3_naics:
            tier3_codes.add(naic.code)
            tier2_codes.add(naic.code[:4])
            tier1_codes.add(naic.code[:2])
        tier2_naics = NAICS.objects.annotate(text_len=Length("code")).filter(**naics_filter, text_len=4)

        for naic in tier2_naics:
            tier2_codes.add(naic.code)
            tier1_codes.add(naic.code[:2])
        extra_tier2_naics = NAICS.objects.annotate(text_len=Length("code")).filter(code__in=tier2_codes, text_len=4)
        tier1_naics = NAICS.objects.annotate(text_len=Length("code")).filter(text_len=2, code__in=tier1_codes)

        tier2 = set(list(tier2_naics))
        ex2 = list(extra_tier2_naics)
        for naic in ex2:
            tier2.add(naic)
        tier2_results = {}
        for naic in tier2:
            result = OrderedDict()
            result["naics"] = naic.code
            result["naics_description"] = naic.description
            result["count"] = (
                NAICS.objects.annotate(text_len=Length("code")).filter(code__startswith=naic.code, text_len=6).count()
            )
            result["children"] = []
            tier2_results[naic.code] = result

        for naic in tier3_naics:
            result = OrderedDict()
            result["naics"] = naic.code
            result["naics_description"] = naic.description
            result["count"] = 0
            tier2_results[naic.code[:4]]["children"].append(result)

        tier1_results = {}
        for naic in tier1_naics:
            result = OrderedDict()
            result["naics"] = naic.code
            result["naics_description"] = naic.description
            result["count"] = (
                NAICS.objects.annotate(text_len=Length("code")).filter(code__startswith=naic.code, text_len=6).count()
            )
            result["children"] = []
            tier1_results[naic.code] = result
        for key in tier2_results.keys():
            tier1_results[key[:2]]["children"].append(tier2_results[key])
        results = []
        for key in tier1_results.keys():
            results.append(tier1_results[key])
        response_content = OrderedDict({"results": results})
        return response_content

    def _default_view(self) -> dict:
        naics = NAICS.objects.annotate(text_len=Length("code")).filter(text_len=2)
        results = []
        for naic in naics:
            result = OrderedDict()
            result["naics"] = naic.code
            result["naics_description"] = naic.description
            result["count"] = (
                NAICS.objects.annotate(text_len=Length("code")).filter(code__startswith=naic.code, text_len=6).count()
            )
            results.append(result)
        response_content = OrderedDict({"results": results})
        return response_content

    def _business_logic(self, request_data: dict) -> dict:
        naics_filter = {}
        code = request_data.get("code")
        description = request_data.get("description")

        if not code and not description:
            return self._default_view()
        elif code:
            naics_filter.update({"code": request_data.get("code")})
        elif description:
            naics_filter.update({"description__icontains": description})
            return self._filter_search(naics_filter)

        naics = NAICS.objects.filter(**naics_filter)
        results = []
        for naic in naics:
            result = OrderedDict()
            if len(naic.code) < 6:
                result["naics"] = naic.code
                result["naics_description"] = naic.description
                result["count"] = (
                    NAICS.objects.annotate(text_len=Length("code"))
                    .filter(code__startswith=naic.code, text_len=6)
                    .count()
                )
                result["children"] = self._fetch_children(naic.code)
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
