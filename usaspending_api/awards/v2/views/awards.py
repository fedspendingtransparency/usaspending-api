import logging

from django.db.models import Max
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.awards.models import Award
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
        response = {
            "last_updated": max_update_date.strftime('%m/%d/%Y')
        }

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
            models = [{"key": "generated_unique_award_id", "name": "generated_unique_award_id", "type": "text", "text_type": "search", "optional": False}]
            request_dict = {"generated_unique_award_id": provided_award_id}

        validated_request_data = TinyShield(models).block(request_dict)
        return validated_request_data

    def _business_logic(self, request_dict: dict) -> list:
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
