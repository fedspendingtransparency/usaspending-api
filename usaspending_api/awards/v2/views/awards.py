import logging

from django.db.models import Max
from rest_framework.exceptions import NotFound
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.models import Award
from usaspending_api.awards.v2.data_layer.orm import (
    construct_contract_response,
    construct_idv_response,
    construct_assistance_response,
)
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.models import AwardSearch


logger = logging.getLogger("console")


class AwardLastUpdatedViewSet(APIView):
    """
    Return all award spending by award type for a given fiscal year and agency id.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/awards/last_updated.md"

    @cache_response()
    def get(self, request):
        """Return latest updated date for Awards"""

        max_update_date = Award.objects.all().aggregate(Max("update_date"))["update_date__max"]
        response = {"last_updated": max_update_date.strftime("%m/%d/%Y")}

        return Response(response)


class AwardRetrieveViewSet(APIView):
    """
    This route sends a request to the backend to retrieve data about a specific award
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/awards/award_id.md"

    def _parse_and_validate_request(self, provided_award_id: str) -> dict:
        request_dict = {"generated_unique_award_id": provided_award_id}
        models = [
            {
                "key": "generated_unique_award_id",
                "name": "generated_unique_award_id",
                "type": "text",
                "text_type": "search",
                "optional": False,
            }
        ]
        if str(provided_award_id).isdigit():
            request_dict = {"award_id": int(provided_award_id)}
            models = [{"key": "award_id", "name": "award_id", "type": "integer", "optional": False}]

        validated_request_data = TinyShield(models).block(request_dict)
        return validated_request_data

    def _business_logic(self, request_dict: dict) -> dict:
        award_queryset = AwardSearch.objects.filter(**request_dict)

        if not award_queryset and "generated_unique_award_id" in request_dict:
            # This is handled as a different DB call to avoid the possibility that one Award's current unique key
            # matches the legacy unique key of a different Award.
            logger.info(f"No Award found with: {request_dict}. Checking against legacy unique award key.")
            request_dict = {"generated_unique_award_id_legacy": request_dict["generated_unique_award_id"]}
            award_queryset = AwardSearch.objects.filter(**request_dict)

        if not award_queryset:
            logger.info(f"No Award found with: {request_dict}")
            raise NotFound(f"No Award found with: {request_dict}")

        award = award_queryset[0]

        # Normalize the different logic branches to all provide the Award's PK to the response logic
        request_dict = {"id": award.award_id}

        if award.category == "contract":
            response_content = construct_contract_response(request_dict)
        elif award.category == "idv":
            response_content = construct_idv_response(request_dict)
        else:
            response_content = construct_assistance_response(request_dict)

        return response_content

    @cache_response()
    def get(self, request: Request, requested_award: str) -> Response:
        request_data = self._parse_and_validate_request(requested_award)
        response = self._business_logic(request_data)
        return Response(response)
