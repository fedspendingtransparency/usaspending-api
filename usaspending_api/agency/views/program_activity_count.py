from django.conf import settings
from rest_framework.exceptions import NotFound
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year, generate_fiscal_year
from usaspending_api.common.helpers.generic_helper import convert_string_to_date
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.references.models import ToptierAgency, RefProgramActivity


class ProgramActivityCount(APIView):
    """
    Obtain the count of program activity categories for a specific agency in a
    single fiscal year
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/program_activity/count.md"

    @cache_response()
    def get(self, request: Request, code: str) -> Response:
        request_dict = {"code": code, "fiscal_year": request.query_params.get("fiscal_year", current_fiscal_year())}
        models = [
            {"key": "code", "name": "code", "type": "text", "text_type": "search", "optional": False},
            {
                "key": "fiscal_year",
                "name": "fiscal_year",
                "type": "integer",
                "min": generate_fiscal_year(convert_string_to_date(settings.API_SEARCH_MIN_DATE)),
                "max": current_fiscal_year(),
                "default": current_fiscal_year(),
            },
        ]

        validated_request_data = TinyShield(models).block(request_dict)

        agency = ToptierAgency.objects.filter(toptier_code=validated_request_data["code"]).values().first()

        if not agency:
            raise NotFound(f"Agency with a toptier code of '{code}' does not exist")

        if validated_request_data["fiscal_year"] < 2017:
            # Currently historical data aren't included in this response. Post-MVP functionality to add in the future
            raise UnprocessableEntityException(
                (
                    "Data powering this endpoint was first collected in FY2017 under the DATA Act;"
                    " as such, there is no data available for earlier Fiscal Years."
                )
            )

        count = (
            RefProgramActivity.objects.filter(
                responsible_agency_id=agency["toptier_code"], budget_year=validated_request_data["fiscal_year"],
            )
            .values("program_activity_code")
            .distinct()
            .count()
        )

        return Response({"program_activity_count": count})
