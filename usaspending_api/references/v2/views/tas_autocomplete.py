from copy import deepcopy
from rest_framework.response import Response
from collections import OrderedDict
from rest_framework.views import APIView
from django.db.models import F
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.references.models import CGAC


TS_MODELS = [
    {"name": "filters|ata", "key": "filters|ata", "type": "text", "text_type": "search", "allow_nulls": True},
    {"name": "filters|aid", "key": "filters|aid", "type": "text", "text_type": "search", "allow_nulls": True},
    {"name": "filters|bpoa", "key": "filters|bpoa", "type": "text", "text_type": "search", "allow_nulls": True},
    {"name": "filters|epoa", "key": "filters|epoa", "type": "text", "text_type": "search", "allow_nulls": True},
    {"name": "filters|a", "key": "filters|a", "type": "text", "text_type": "search", "allow_nulls": True},
    {"name": "filters|main", "key": "filters|main", "type": "text", "text_type": "search", "allow_nulls": True},
    {"name": "filters|sub", "key": "filters|sub", "type": "text", "text_type": "search", "allow_nulls": True},
    {"name": "limit", "key": "limit", "type": "integer", "max": 500, "default": 10},
]


COMPONENT_MAPPING = {
    "ata": "allocation_transfer_agency_id",
    "aid": "agency_id",
    "bpoa": "beginning_period_of_availability",
    "epoa": "ending_period_of_availability",
    "a": "availability_type_code",
    "main": "main_account_code",
    "sub": "sub_account_code",
}


class TASAutocomplete(APIView):

    @staticmethod
    def _parse_and_validate_request(request_data):
        return TinyShield(deepcopy(TS_MODELS)).block(request_data)

    @staticmethod
    def _business_logic(filters, requested_component, limit):

        requested_column = COMPONENT_MAPPING[requested_component]
        kwargs = {}
        for current_component, current_value in filters.items():
            current_column = COMPONENT_MAPPING[current_component]
            if current_value is None:
                kwargs[current_column + "__isnull"] = True
            elif current_column == requested_column:
                kwargs[current_column + "__startswith"] = current_value
            else:
                kwargs[current_column] = current_value

        results = (
            TreasuryAppropriationAccount.objects.filter(**kwargs)
            .values(**{requested_component: F(requested_column)})
            .distinct()
            .order_by(requested_column)[:limit]
        )

        if requested_component in ("ata", "aid"):

            # Convert to ordered dictionary so the columns are returned in the same order every time.
            results = [OrderedDict(d) for d in results]

            # Look up the agency name and abbreviation for ata and aid.
            cgacs = CGAC.objects.filter(cgac_code__in=[r[requested_component] for r in results])

            # Create lookups for agency names and abbreviations.
            agency_names = {cgac.cgac_code: cgac.agency_name for cgac in cgacs}
            agency_abbreviations = {cgac.cgac_code: cgac.agency_abbreviation for cgac in cgacs}

            # Stuff the agency names and abbreviations into our result set.
            for result in results:
                key = result.get(requested_component)
                result["agency_name"] = agency_names.get(key)
                result["agency_abbreviation"] = agency_abbreviations.get(key)

        return results

    @cache_response()
    def post(self, request, requested_component):
        request_data = self._parse_and_validate_request(request.data)
        results = self._business_logic(request_data.get("filters", {}), requested_component, request_data["limit"])
        return Response(results)
