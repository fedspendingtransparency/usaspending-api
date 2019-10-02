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

    def _fetch_parents(self, naics_code) -> dict:
        parent = (
            NAICS.objects.annotate(text_len=Length("code")).filter(code__startswith=naics_code[:2], text_len=2).first()
        )
        results = OrderedDict()
        results["naics"] = parent.code
        results["naics_description"] = parent.description
        results["count"] = (
            NAICS.objects.annotate(text_len=Length("code")).filter(code__startswith=parent.code, text_len=6).count()
        )
        results["children"] = []

        if len(naics_code) == 6:
            parent2 = (
                NAICS.objects.annotate(text_len=Length("code"))
                .filter(code__startswith=naics_code[:4], text_len=4)
                .first()
            )
            results2 = OrderedDict()
            results2["naics"] = parent2.code
            results2["naics_description"] = parent2.description
            results2["count"] = (
                NAICS.objects.annotate(text_len=Length("code"))
                .filter(code__startswith=parent2.code, text_len=6)
                .count()
            )
            results2["children"] = []
            results["children"].append(results2)

        return results

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
        naics = NAICS.objects.annotate(text_len=Length("code")).filter(**naics_filter).exclude(text_len=2)
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
                children = self._fetch_children(
                    naic.code, {"description__icontains": naics_filter["description__icontains"]}
                )
                if len(children) > 0:
                    result["children"] = children
                parent = self._fetch_parents(naic.code)
                parent["children"].append(result)
                result = parent
            else:
                result["naics"] = naic.code
                result["naics_description"] = naic.description
                result["count"] = 0
                parent = self._fetch_parents(naic.code)
                parent["children"][0]["children"].append(result)
                result = parent
            results.append(result)

        response = {}
        tier1_codes = []
        for result in results:
            if len(result["naics"]) == 2:
                if result["naics"] in tier1_codes:
                    existing_result = response[result["naics"]]
                    for child in result["children"]:
                        exists = False
                        for kid in existing_result["children"]:
                            if child["naics"] == kid["naics"]:
                                exists = True
                                for c in child["children"]:
                                    kid["children"].append(c)

                        if not exists:
                            existing_result["children"].append(child)

                else:
                    tier1_codes.append(result["naics"])
                    response.update({result["naics"]: result})
        ret = []
        for key in response.keys():
            ret.append(response[key])
        response_content = OrderedDict({"results": ret})
        return response_content

    def _business_logic(self, request_data: dict) -> dict:
        naics_filter = {}
        code = request_data.get("code")
        description = request_data.get("description")

        if not code and not description:  # we just want the tier 1 naics codes
            naics = NAICS.objects.annotate(text_len=Length("code")).filter(text_len=2)
            results = []
            for naic in naics:
                result = OrderedDict()
                result["naics"] = naic.code
                result["naics_description"] = naic.description
                result["count"] = (
                    NAICS.objects.annotate(text_len=Length("code"))
                    .filter(code__startswith=naic.code, text_len=6)
                    .count()
                )
                results.append(result)
            response_content = OrderedDict({"results": results})
            return response_content

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
                result["count"] = 0
            results.append(result)

        response_content = OrderedDict({"results": results})
        return response_content

    @cache_response()
    def get(self, request: Request, requested_naics: str = None) -> Response:
        request_data = self._parse_and_validate_request(requested_naics, request.GET)
        results = self._business_logic(request_data)
        return Response(results)
