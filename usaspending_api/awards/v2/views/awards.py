import logging

from collections import OrderedDict

from django.db.models import Max
from rest_framework.exceptions import NotFound
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.awards.models import Award, ParentAward
from usaspending_api.awards.v2.data_layer.orm import (
    construct_contract_response,
    construct_idv_response,
    construct_assistance_response,
)
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.core.validator.tinyshield import TinyShield

logger = logging.getLogger("console")


class AwardLastUpdatedViewSet(APIDocumentationView):
    """Return all award spending by award type for a given fiscal year and agency id.
    endpoint_doc: /awards/last_updated.md
    """

    @cache_response()
    def get(self, request):
        """Return latest updated date for Awards"""

        max_update_date = Award.objects.all().aggregate(Max('update_date'))['update_date__max']
        response = {"last_updated": max_update_date.strftime('%m/%d/%Y')}

        return Response(response)


class AwardRetrieveViewSet(APIDocumentationView):
    """
    endpoint_doc: /awards.md
    """

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
            request_dict = {"id": int(provided_award_id)}
            models = [{"key": "id", "name": "id", "type": "integer", "optional": False}]

        validated_request_data = TinyShield(models).block(request_dict)
        return validated_request_data

    def _business_logic(self, request_dict: dict) -> dict:
        try:
            award = Award.objects.get(**request_dict)
        except Award.DoesNotExist:
            logger.info("No Award found with: '{}'".format(request_dict))
            raise NotFound("No Award found with: '{}'".format(request_dict))

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


class IDVAmountsViewSet(APIDocumentationView):
    """Returns counts and dollar figures for a specific IDV.
    endpoint_doc: /awards/idvs/amounts.md
    """

    @staticmethod
    def _parse_and_validate_request(requested_award: str) -> dict:
        if requested_award.isdigit():
            request_dict = {'award_id': int(requested_award)}
            models = [{'key': 'award_id', 'name': 'award_id', 'type': 'integer', 'optional': False}]
        else:
            request_dict = {'generated_unique_award_id': requested_award.upper()}
            models = [{
                'key': 'generated_unique_award_id',
                'name': 'generated_unique_award_id',
                'type': 'text',
                'text_type': 'raw',
                'optional': False,
            }]
        return TinyShield(models).block(request_dict)

    @staticmethod
    def _business_logic(request_data: dict) -> OrderedDict:
        try:
            parent_award = ParentAward.objects.get(**request_data)
            return OrderedDict((
                ('award_id', parent_award.award_id),
                ('generated_unique_award_id', parent_award.generated_unique_award_id),
                ('idv_count', parent_award.direct_idv_count),
                ('contract_count', parent_award.direct_contract_count),
                ('rollup_total_obligation', parent_award.rollup_total_obligation),
                ('rollup_base_and_all_options_value', parent_award.rollup_base_and_all_options_value),
                ('rollup_base_exercised_options_val', parent_award.rollup_base_exercised_options_val),
            ))
        except ParentAward.DoesNotExist:
            logger.info("No IDV Award found where '%s' is '%s'" % next(iter(request_data.items())))
            raise NotFound("No IDV award found with this id")

    @cache_response()
    def get(self, request: Request, requested_award: str) -> Response:
        """Return IDV counts and sums"""
        request_data = self._parse_and_validate_request(requested_award)
        return Response(self._business_logic(request_data))
