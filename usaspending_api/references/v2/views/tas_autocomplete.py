from collections import OrderedDict
from copy import deepcopy
from rest_framework.response import Response
from rest_framework.views import APIView
from usaspending_api.accounts.helpers import TAS_COMPONENT_TO_FIELD_MAPPING
from usaspending_api.accounts.models import TASAutocompleteMatview
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.references.models import CGAC


TINY_SHIELD_MODELS = [
    {"name": "filters|ata", "key": "filters|ata", "type": "text", "text_type": "search", "allow_nulls": True},
    {"name": "filters|aid", "key": "filters|aid", "type": "text", "text_type": "search", "allow_nulls": True},
    {"name": "filters|bpoa", "key": "filters|bpoa", "type": "text", "text_type": "search", "allow_nulls": True},
    {"name": "filters|epoa", "key": "filters|epoa", "type": "text", "text_type": "search", "allow_nulls": True},
    {"name": "filters|a", "key": "filters|a", "type": "text", "text_type": "search", "allow_nulls": True},
    {"name": "filters|main", "key": "filters|main", "type": "text", "text_type": "search", "allow_nulls": True},
    {"name": "filters|sub", "key": "filters|sub", "type": "text", "text_type": "search", "allow_nulls": True},
    {"name": "limit", "key": "limit", "type": "integer", "max": 500, "default": 10},
]


class TASAutocomplete(APIView):
    """
    This endpoint supports all of the various TAS autocomplete components (ATA, AID, BPOA, EPOA, A, MAIN, SUB).
    """
    endpoint_doc = "usaspending_api/api_contracts/contracts/autocomplete/accounts/aid.md"

    @staticmethod
    def _parse_and_validate_request(request_data):
        return TinyShield(deepcopy(TINY_SHIELD_MODELS)).block(request_data)

    @staticmethod
    def _business_logic(filters, requested_component, limit):

        requested_column = TAS_COMPONENT_TO_FIELD_MAPPING[requested_component]
        kwargs = {}
        for current_component, current_value in filters.items():
            current_column = TAS_COMPONENT_TO_FIELD_MAPPING[current_component]
            if current_value is None:
                kwargs[current_column + "__isnull"] = True
            elif current_column == requested_column:
                kwargs[current_column + "__startswith"] = current_value
            else:
                kwargs[current_column] = current_value

        results = (
            TASAutocompleteMatview.objects.filter(**kwargs)
            .values_list(requested_column, flat=True)
            .distinct()
            .order_by(requested_column)[:limit]
        )

        if requested_component in ("ata", "aid"):

            # Look up the agency names and abbreviations for ata and aid.
            cgacs = CGAC.objects.filter(cgac_code__in=results)

            # Create lookups for agency names and abbreviations.
            agency_names = {cgac.cgac_code: cgac.agency_name for cgac in cgacs}
            agency_abbreviations = {cgac.cgac_code: cgac.agency_abbreviation for cgac in cgacs}

            # Build a new result set with the requested_component, agency_name,
            # and agency_abbreviation.
            results = [
                OrderedDict(
                    [
                        (requested_component, r),
                        ("agency_name", agency_names.get(r)),
                        ("agency_abbreviation", agency_abbreviations.get(r)),
                    ]
                )
                for r in results
            ]

        return {"results": results}

    @cache_response()
    def post(self, request, requested_component):
        request_data = self._parse_and_validate_request(request.data)
        results = self._business_logic(request_data.get("filters", {}), requested_component, request_data["limit"])
        return Response(results)
