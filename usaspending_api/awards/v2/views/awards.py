from rest_framework.response import Response
from rest_framework.request import Request
from django.db.models import Max

from usaspending_api.awards.models import Award
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.core.validator.tinyshield import TinyShield
from usaspending_api.awards.serializers_v2.serializers import AwardContractSerializerV2, AwardMiscSerializerV2


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

    def _parse_and_validate_request(self, generated_unique_award_id) -> dict:

        # TODO:  May be better to accept either generated_unique_award_id or
        #       internal pk to ease transition from v1 to v2

        models = [{"key": "generated_unique_award_id", "name": "generated_unique_award_id", "type": "text",
                   "text_type": "search", "optional": False}]
        request_dict = {"generated_unique_award_id": generated_unique_award_id}
        validated_request_data = TinyShield(models).block(request_dict)
        return validated_request_data

    def _business_logic(self, generated_unique_award_id: str) -> list:
        try:
            award = Award.objects.get(generated_unique_award_id=generated_unique_award_id)
            if award.category == 'contract':
                parent_recipient_name = award.latest_transaction.contract_data.ultimate_parent_legal_enti
                serialized = AwardContractSerializerV2(award).data
                serialized['recipient']['parent_recipient_name'] = parent_recipient_name
                return serialized
            else:
                parent_recipient_name = award.latest_transaction.assistance_data.ultimate_parent_legal_enti
                serialized = AwardMiscSerializerV2(award).data
                serialized['recipient']['parent_recipient_name'] = parent_recipient_name
                return serialized
        except Award.DoesNotExist:
            return {"message": "No award found with this id"}

    @cache_response()
    def get(self, request: Request, generated_unique_award_id: str) -> Response:
        request_data = self._parse_and_validate_request(generated_unique_award_id)
        response = self._business_logic(request_data['generated_unique_award_id'])
        return Response(response)
