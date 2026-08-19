from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.references.models import RefCountryCode


class CountriesViewSet(APIView):
    """
    This route returns countries from the ref_country_code table
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/countries.md"

    @cache_response()
    def get(self, request: Request) -> Response:
        # ref_country_code stores country codes and names
from django.db.models import CharField, Value
from django.db.models.functions import Coalesce, Upper

results = list(
    RefCountryCode.objects
    .annotate(
        code=Upper(Coalesce("country_code", Value(""), output_field=CharField())),
        name=Upper(Coalesce("country_name", Value(""), output_field=CharField()))
    )
    .values("code", "name")
    .order_by("code")
)
        return Response({"results": results})
