from collections import OrderedDict
from copy import deepcopy
from rest_framework.response import Response
from rest_framework.views import APIView
from usaspending_api.accounts.helpers import TAS_COMPONENT_TO_FIELD_MAPPING
from usaspending_api.search.models import TASAutocompleteMatview
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
    THIS IS AN ABSTRACT CLASS.  DO NOT INSTANTIATE DIRECTLY.  USE ONE OF THE
    SUBCLASS FLAVORS BELOW.

    This class supports all of the TAS autocomplete endpoints (ATA, AID, BPOA,
    EPOA, A, MAIN, SUB).  The reason we split this up vs. service all TAS
    components from a single view is purely for documentation reasons.
    Each TAS component has slightly different documentation requirements
    and since documentation is tied to the view class, we need a class per
    endpoint.
    """

    @staticmethod
    def _parse_and_validate_request(request_data):
        return TinyShield(deepcopy(TINY_SHIELD_MODELS)).block(request_data)

    def _business_logic(self, filters, limit):

        requested_column = TAS_COMPONENT_TO_FIELD_MAPPING[self.component]
        kwargs = {}
        for current_component, current_value in filters.items():
            current_column = TAS_COMPONENT_TO_FIELD_MAPPING[current_component]
            if current_value is None:
                kwargs[current_column + "__isnull"] = True
            elif current_column == requested_column:
                kwargs[current_column + "__startswith"] = current_value
            else:
                kwargs[current_column] = current_value

        results = list(
            TASAutocompleteMatview.objects.filter(**kwargs)
            .values_list(requested_column, flat=True)
            .distinct()
            .order_by(requested_column)[:limit]
        )

        if self.component in ("ata", "aid"):

            # Look up the agency names and abbreviations for ata and aid.
            cgacs = CGAC.objects.filter(cgac_code__in=results)

            # Create lookups for agency names and abbreviations.
            agency_names = {cgac.cgac_code: cgac.agency_name for cgac in cgacs}
            agency_abbreviations = {cgac.cgac_code: cgac.agency_abbreviation for cgac in cgacs}

            # Build a new result set with the component, agency_name,
            # and agency_abbreviation.
            results = [
                OrderedDict(
                    [
                        (self.component, r),
                        ("agency_name", agency_names.get(r)),
                        ("agency_abbreviation", agency_abbreviations.get(r)),
                    ]
                )
                for r in results
            ]

        return {"results": results}

    @cache_response()
    def post(self, request):
        request_data = self._parse_and_validate_request(request.data)
        results = self._business_logic(request_data.get("filters", {}), request_data["limit"])
        return Response(results)


class TASAutocompleteATA(TASAutocomplete):
    """
    Returns the list of potential Allocation Transfer Agency Identifiers
    narrowed by other components supplied in the Treasury Account filter.
    """

    component = "ata"
    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/accounts/ata.md"


class TASAutocompleteAID(TASAutocomplete):
    """
    Returns the list of potential Agency Identifiers narrowed by other
    components supplied in the Treasury Account or Federal Account filter.
    """

    component = "aid"
    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/accounts/aid.md"


class TASAutocompleteBPOA(TASAutocomplete):
    """
    Returns the list of potential Beginning Period of Availabilities
    narrowed by other components supplied in the Treasury Account filter.
    """

    component = "bpoa"
    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/accounts/bpoa.md"


class TASAutocompleteEPOA(TASAutocomplete):
    """
    Returns the list of potential Ending Period of Availabilities
    narrowed by other components supplied in the Treasury Account filter.
    """

    component = "epoa"
    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/accounts/epoa.md"


class TASAutocompleteA(TASAutocomplete):
    """
    Returns the list of potential Availability Type Codes
    narrowed by other components supplied in the Treasury Account filter.
    """

    component = "a"
    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/accounts/a.md"


class TASAutocompleteMAIN(TASAutocomplete):
    """
    Returns the list of potential Main Account Codes narrowed by other
    components supplied in the Treasury Account or Federal Account filter.
    """

    component = "main"
    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/accounts/main.md"


class TASAutocompleteSUB(TASAutocomplete):
    """
    Returns the list of potential Sub Account Codes
    narrowed by other components supplied in the Treasury Account filter.
    """

    component = "sub"
    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/accounts/sub.md"
