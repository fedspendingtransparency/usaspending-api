from django.db.models import Q
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.validator import TinyShield
from usaspending_api.references.models import Cfda


class AssistanceListingViewSet(APIView):
    """Return a list of Assistance Listings or a filtered list of Assistance Listings"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/assistance_listing.md"

    queryset = Cfda.objects.all()

    def _parse_and_validate_request(self, cfda: int, requested_data) -> dict:
        data = {"code": cfda, "filter": requested_data.query_params.get("filter", None)}
        models = [
            {"key": "code", "name": "code", "type": "integer", "default": None, "allow_nulls": True, "optional": True},
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
        return TinyShield(models).block(data)

    def _business_logic(self, cfda_code: int, cfda_filter: str) -> list:
        valid_cfda_code = True if len(str(cfda_code)) == 2 else False
        if valid_cfda_code:
            self.queryset = Cfda.objects.filter(program_number__startswith=cfda_code)

        elif cfda_code is not None:
            raise InvalidParameterException(f"The assistance listing code should be two digits or not provided at all")

        all_cfda_program_numbers = self.queryset.values_list("program_number", flat=True)
        prefixes = set(cfda.split(".")[0] for cfda in all_cfda_program_numbers if "." in cfda)
        cfdas = []
        for prefix in prefixes:
            filter_query = (
                Cfda.objects.filter(Q(program_number__contains=cfda_filter) | Q(program_title__contains=cfda_filter))
                if cfda_filter is not None
                else Cfda.objects.all()
            )
            cfda_with_prefix = filter_query.filter(program_number__startswith=prefix).values(
                "program_number", "program_title"
            )
            children = [
                {"code": cfda["program_number"], "description": cfda["program_title"]} for cfda in cfda_with_prefix
            ]
            if valid_cfda_code:
                cfdas.append(
                    {
                        "code": prefix,
                        "description": None,
                        "count": len(children),
                        "children": children,
                    }
                )
            else:
                cfdas.append(
                    {
                        "code": prefix,
                        "description": None,
                        "count": len(children),
                    }
                )

        return sorted(cfdas, key=lambda cfda: cfda["code"])

    @cache_response()
    def get(self, request, cfda=None) -> Response:
        requested_data = self._parse_and_validate_request(cfda, request)
        results = self._business_logic(requested_data["code"], requested_data["filter"])
        return Response(results)
