from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models.functions import Cast
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView
from django.db.models import Func, F, TextField, Value, Count, JSONField, QuerySet, CharField
from django.db.models.query import Q
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.validator import TinyShield, customize_pagination_with_sort_columns
from usaspending_api.references.models import Cfda


class AssistanceListingViewSet(APIView):
    """Return a list of Assistance Listings or a filtered list of Assistance Listings"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/assistance_listing.md"

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

        if cfda is not None and (len(str(cfda)) != 2 or not cfda.isdigit()):
            raise InvalidParameterException(f"The assistance listing code should be two digits or not provided at all")

        return TinyShield(models).block(data)

    def _business_logic(self, cfda_code: int | None, cfda_filter: str | None, page: Pagination) -> QuerySet:
        qs = Cfda.objects.all()
        annotations = {"description": Value(None, output_field=CharField()), "count": Count("code")}
        if cfda_filter:
            qs = qs.filter(Q(program_title__icontains=cfda_filter) | Q(program_number__icontains=cfda_filter))

        if cfda_code is None and cfda_filter:
            annotations = {
                "description": Value(None, output_field=CharField()),
                "count": Count("code"),
                "children": ArrayAgg(
                    Func(
                        Cast(Value("code"), TextField()),
                        F("program_number"),
                        Cast(Value("description"), TextField()),
                        F("program_title"),
                        function="jsonb_build_object",
                        output_field=JSONField(),
                    )
                ),
            }
        elif cfda_code:
            qs = qs.filter(program_number__startswith=cfda_code)
            annotations = {"code": F("program_number"), "description": F("program_title")}
            results = qs.annotate(**annotations).values(**annotations).order_by(*page.robust_order_by_fields)
            return results

        results = (
            qs.annotate(
                code=Func(F("program_number"), function="SPLIT_PART", template="%(function)s(%(expressions)s, '.', 1)")
            )
            .values("code")
            .annotate(**annotations)
            .order_by(*page.robust_order_by_fields)
        )

        return results

    @cache_response()
    def get(self, request: Request, cfda=None) -> Response:
        requested_data = self._parse_and_validate_request(cfda, request)
        page = PaginationMixin().pagination(request)
        results = self._business_logic(requested_data["code"], requested_data["filter"], page)
        page_metadata = get_pagination_metadata(len(results), page.limit, page.page)
        if cfda:
            return Response({"results": results[page.lower_limit : page.upper_limit], "page_metadata": page_metadata})
        if requested_data["filter"] and not cfda:
            return Response(
                {
                    "results": results,
                    "message": 'Pagination is ignored when providing a "filter" without specifying the first two digits of an assistance listing code',
                }
            )
        return Response(results)


class PaginationMixin:

    def pagination(self, request: Request) -> Pagination:
        sortable_columns = ["code", "description"]
        default_sort_column = "code"
        model = customize_pagination_with_sort_columns(sortable_columns, default_sort_column)
        request_data = (
            TinyShield(model).block(request.data)
            if request.method == "POST"
            else TinyShield(model).block(request.query_params)
        )

        primary_sort_key = request_data.get("sort", default_sort_column)

        return Pagination(
            page=request_data["page"],
            limit=request_data["limit"],
            lower_limit=(request_data["page"] - 1) * request_data["limit"],
            upper_limit=(request_data["page"] * request_data["limit"]),
            sort_key=primary_sort_key,
            sort_order=request_data["order"],
        )
