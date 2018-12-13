import logging

from collections import OrderedDict

from django.db.models import Max
from rest_framework import status
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.awards.models import Award, ParentAward
from usaspending_api.awards.serializers_v2.serializers import AwardContractSerializerV2, AwardMiscSerializerV2
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import NotImplementedException, UnprocessableEntityException
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
        try:
            request_dict = {"id": int(provided_award_id)}
            models = [{"key": "id", "name": "id", "type": "integer", "optional": False}]
        except Exception as e:
            models = [
                {
                    "key": "generated_unique_award_id",
                    "name": "generated_unique_award_id",
                    "type": "text",
                    "text_type": "search",
                    "optional": False,
                }
            ]
            request_dict = {"generated_unique_award_id": provided_award_id}

        validated_request_data = TinyShield(models).block(request_dict)
        return validated_request_data

    def _business_logic(self, request_dict: dict) -> dict:
        dict_key = "id" if "id" in request_dict else "generated_unique_award_id"

        try:
            award = Award.objects.get(**{dict_key: request_dict[dict_key]})
        except Award.DoesNotExist:
            logger.info("No Award found with '{}' in '{}'".format(request_dict[dict_key], dict_key))
            return {"message": "No award found with this id"}  # Consider returning 404 or 410 error code

        try:
            if award.category == 'contract':
                parent_recipient_name = award.latest_transaction.contract_data.ultimate_parent_legal_enti
                serialized = AwardContractSerializerV2(award).data
                serialized['recipient']['parent_recipient_name'] = parent_recipient_name
            elif award.category == "idv":
                raise NotImplementedException("IDVs are not yet implemented")
            else:
                parent_recipient_name = award.latest_transaction.assistance_data.ultimate_parent_legal_enti
                serialized = AwardMiscSerializerV2(award).data
                serialized['recipient']['parent_recipient_name'] = parent_recipient_name
        except AttributeError:
            raise UnprocessableEntityException("Unable to complete response due to missing Award data")

        return serialized

    @cache_response()
    def get(self, request: Request, requested_award: str) -> Response:
        request_data = self._parse_and_validate_request(requested_award)
        response = self._business_logic(request_data)
        return Response(response)


class IDVAmountsViewSet(APIDocumentationView):
    """Return IDV values from parent_award table.
    endpoint_doc: /awards/idvs/amounts.md
    """

    @staticmethod
    def _parse_and_validate_request(requested_award: str) -> dict:
        try:
            request_dict = {'award_id': int(requested_award)}
            models = [{
                'key': 'award_id',
                'name': 'award_id',
                'type': 'integer',
                'optional': False,
            }]
        except ValueError:
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
    def _business_logic(request_data: dict) -> dict:
        try:
            parent_award = ParentAward.objects.get(**request_data)
            return {
                'data': OrderedDict((
                    ('award_id', parent_award.award_id),
                    ('generated_unique_award_id', parent_award.generated_unique_award_id),
                    ('idv_count', parent_award.direct_idv_count),
                    ('contract_count', parent_award.direct_contract_count),
                    ('rollup_total_obligation', parent_award.rollup_total_obligation),
                    ('rollup_base_and_all_options_value', parent_award.rollup_base_and_all_options_value),
                    ('rollup_base_exercised_options_val', parent_award.rollup_base_exercised_options_val),
                )),
                'status': status.HTTP_200_OK
            }
        except ParentAward.DoesNotExist:
            logger.info("No IDV Award found where '%s' is '%s'" % next(iter(request_data.items())))
            return {
                'data': OrderedDict({'message': 'No IDV award found with this id'}),
                'status': status.HTTP_404_NOT_FOUND
            }

    @cache_response()
    def get(self, request: Request, requested_award: str) -> Response:
        """Return IDV counts and sums"""
        request_data = self._parse_and_validate_request(requested_award)
        return Response(**self._business_logic(request_data))
