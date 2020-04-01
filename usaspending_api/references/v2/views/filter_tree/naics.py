import logging
from collections import OrderedDict

from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView
from django.db.models.functions import Length
from django.db.models import Q

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.references.models import NAICS
from usaspending_api.references.v2.views.filter_trees.filter_tree import DEFAULT_CHILDREN

logger = logging.getLogger("console")


class NAICSViewSet(APIView):
    """
    Return a list of NAICS or a filtered list of NAICS
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/naics.md"

    def _parse_and_validate_request(self, requested_naics: str, request_data) -> dict:
        naics_filter = request_data.get("filter")
        data = {"code": requested_naics, "filter": naics_filter}
        models = [
            {"key": "code", "name": "code", "type": "integer", "allow_nulls": True, "optional": True},
            {
                "key": "filter",
                "name": "filter",
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
                result["count"] = DEFAULT_CHILDREN
            results.append(result)
        results.sort(key=lambda x: x["naics"])
        return results

    def _filter_search(self, naics_filter: dict) -> dict:
        search_filter = Q(description__icontains=naics_filter["description__icontains"])
        search_filter |= Q(code__icontains=naics_filter["description__icontains"])
        if naics_filter.get("code"):
            search_filter &= Q(code__startswith=naics_filter["code"])
        tier1_codes = set()
        tier2_codes = set()
        tier3_codes = set()
        naics = list(NAICS.objects.annotate(text_len=Length("code")).filter(search_filter))
        tier3_naics = [naic for naic in naics if len(naic.code) == 6]
        tier2_naics = [naic for naic in naics if len(naic.code) == 4]
        tier1_naics = [naic for naic in naics if len(naic.code) == 2]
        for naic in tier3_naics:
            tier3_codes.add(naic.code)
            tier2_codes.add(naic.code[:4])
            tier1_codes.add(naic.code[:2])

        for naic in tier2_naics:
            tier2_codes.add(naic.code)
            tier1_codes.add(naic.code[:2])

        extra_tier2_naics = NAICS.objects.annotate(text_len=Length("code")).filter(code__in=tier2_codes, text_len=4)
        extra_tier1_naics = NAICS.objects.annotate(text_len=Length("code")).filter(code__in=tier1_codes, text_len=2)
        tier2 = set(list(tier2_naics)) | set(list(extra_tier2_naics))
        tier1 = set(list(tier1_naics)) | set(list(extra_tier1_naics))
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
            result["count"] = DEFAULT_CHILDREN
            tier2_results[naic.code[:4]]["children"].append(result)
            tier2_results[naic.code[:4]]["children"].sort(key=lambda x: x["naics"])
        tier1_results = {}
        for naic in tier1:
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
            tier1_results[key[:2]]["children"].sort(key=lambda x: x["naics"])
        results = []
        for key in tier1_results.keys():
            results.append(tier1_results[key])
        results.sort(key=lambda x: x["naics"])
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
        results.sort(key=lambda x: x["naics"])
        response_content = OrderedDict({"results": results})
        return response_content

    def _business_logic(self, request_data: dict) -> dict:
        naics_filter = {}
        code = request_data.get("code")
        description = request_data.get("filter")

        if not code and not description:
            return self._default_view()
        if code:
            naics_filter.update({"code": request_data.get("code")})
        if description:
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
                result["count"] = DEFAULT_CHILDREN
            results.append(result)

        response_content = OrderedDict({"results": results})
        return response_content

    @cache_response()
    def get(self, request: Request, requested_naics: str = None) -> Response:
        request_data = self._parse_and_validate_request(requested_naics, request.GET)
        results = self._business_logic(request_data)
        return Response(results)
