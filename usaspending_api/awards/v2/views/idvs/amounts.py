import logging

from collections import OrderedDict

from rest_framework.exceptions import NotFound
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.awards.models import ParentAward
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.common.validator.award import get_internal_or_generated_award_id_model
from usaspending_api.common.validator.tinyshield import TinyShield


logger = logging.getLogger('console')


class IDVAmountsViewSet(APIDocumentationView):
    """Returns counts and dollar figures for a specific IDV.
    endpoint_doc: /awards/idvs/amounts.md
    """

    @staticmethod
    def _parse_and_validate_request(requested_award: str) -> dict:
        return TinyShield([get_internal_or_generated_award_id_model()]).block({'award_id': requested_award})

    @staticmethod
    def _business_logic(request_data: dict) -> OrderedDict:
        # By this point, our award_id has been validated and cleaned up by
        # TinyShield.  We will either have an internal award id that is an
        # integer or a generated award id that is a string.
        award_id = request_data['award_id']
        award_id_column = 'award_id' if type(award_id) is int else 'generated_unique_award_id'

        try:
            parent_award = ParentAward.objects.get(**{award_id_column: award_id})
            return OrderedDict((
                ('award_id', parent_award.award_id),
                ('generated_unique_award_id', parent_award.generated_unique_award_id),
                ('child_idv_count', parent_award.direct_idv_count),
                ('child_award_count', parent_award.direct_contract_count),
                ('child_award_total_obligation', parent_award.direct_total_obligation),
                ('child_award_base_and_all_options_value', parent_award.direct_base_and_all_options_value),
                ('child_award_base_exercised_options_val', parent_award.direct_base_exercised_options_val),
                ('grandchild_award_count', parent_award.rollup_contract_count - parent_award.direct_contract_count),
                ('grandchild_award_total_obligation',
                    parent_award.rollup_total_obligation - parent_award.direct_total_obligation),
                ('grandchild_award_base_and_all_options_value',
                    parent_award.rollup_base_and_all_options_value - parent_award.direct_base_and_all_options_value),
                ('grandchild_award_base_exercised_options_val',
                    parent_award.rollup_base_exercised_options_val - parent_award.direct_base_exercised_options_val),
            ))
        except ParentAward.DoesNotExist:
            logger.info("No IDV Award found where '%s' is '%s'" % next(iter(request_data.items())))
            raise NotFound("No IDV award found with this id")

    @cache_response()
    def get(self, request: Request, requested_award: str) -> Response:
        request_data = self._parse_and_validate_request(requested_award)
        return Response(self._business_logic(request_data))
