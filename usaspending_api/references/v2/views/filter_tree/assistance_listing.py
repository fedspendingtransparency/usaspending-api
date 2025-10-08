from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models.functions import Cast
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView
from django.db.models import Func, F, TextField, Value, Count, JSONField
from django.db.models.query import Q
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.validator import TinyShield
from usaspending_api.references.models import Cfda


class AssistanceListingViewSet(APIView):
    """Return a list of Assistance Listings or a filtered list of Assistance Listings"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/assistance_listing.md"

    all_cfdas = Cfda.objects.all()

    def _parse_and_validate_request(self, cfda: str | None, requested_data: Request) -> dict[str, int | str | None]:
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

    def _business_logic(self, cfda_code: int | None, cfda_filter: str | None) -> list[dict]:
        filter_condition = (
            (Q(program_title__contains=cfda_filter) | Q(program_number__contains=cfda_filter)) if cfda_filter else Q()
        )
        annotations = {}
        cfda_filters = (Q(program_number__startswith=cfda_code)) if cfda_code else Q()
        valid_cfda_code = True if len(str(cfda_code)) == 2 else False
        if valid_cfda_code:
            annotations["children"] = ArrayAgg(
                Func(
                    Cast(Value("code"), TextField()),
                    F("program_number"),
                    Cast(Value("description"), TextField()),
                    F("program_title"),
                    function="jsonb_build_object",
                    output_field=JSONField(),
                )
            )
        elif cfda_code is not None:
            raise InvalidParameterException(f"The assistance listing code should be two digits or not provided at all")

        results = (
            Cfda.objects.filter(filter_condition, cfda_filters)
            .annotate(
                code=Func(F("program_number"), function="SPLIT_PART", template="%(function)s(%(expressions)s, '.', 1)")
            )
            .values("code")
            .annotate(count=Count("code"))
            .annotate(**annotations)
        )

        return sorted(results, key=lambda cfda: cfda["code"])

    @cache_response()
    def get(self, request: Request, cfda=None) -> Response:
        requested_data = self._parse_and_validate_request(cfda, request)
        results = self._business_logic(requested_data["code"], requested_data["filter"])
        return Response(results)
