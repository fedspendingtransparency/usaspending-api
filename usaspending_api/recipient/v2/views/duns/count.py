from rest_framework.views import APIView
from rest_framework.response import Response
from django.db.models import Q

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.recipient.models import RecipientProfile
from usaspending_api.recipient.v2.lookups import AWARD_TYPES, SPECIAL_CASES


def duns_count_key_function(view_instance, view_method, request, args, kwargs):
    validated_payload = TinyShield(view_instance.models).block(request.data)

    award_type = "-"
    if "award_type" in validated_payload:
        award_type = validated_payload["award_type"]

    keyword = "-"
    if "keyword" in validated_payload:
        keyword = validated_payload["keyword"]

    key = '.'.join([
        award_type,
        keyword,
    ])

    return key


class DunsCount(APIView):
    """
    This route takes a single keyword filter and agency_type, and returns a count of matching recipients
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/recipient/duns/count.md"

    award_types = list(AWARD_TYPES.keys()) + ["all"]
    models = [
        {"name": "keyword", "key": "keyword", "type": "text", "text_type": "search"},
        {"name": "award_type", "key": "award_type", "type": "enum", "enum_values": award_types, "default": "all"},
    ]

    def get_count(self, filters={}):

        qs_filter = Q()
        if "keyword" in filters:
            qs_filter |= Q(recipient_name__contains=filters["keyword"].upper())
            qs_filter |= Q(recipient_unique_id__contains=filters["keyword"])

        amount_column = "last_12_months"
        if filters["award_type"] != "all":
            amount_column = AWARD_TYPES[filters["award_type"]]["amount"]
            qs_filter &= Q(award_types__overlap=[AWARD_TYPES[filters["award_type"]]["filter"]])

        queryset = (
            RecipientProfile.objects.filter(qs_filter)
            .values("recipient_level", "recipient_hash", "recipient_unique_id", "recipient_name", amount_column)
            .exclude(recipient_name__in=SPECIAL_CASES)
        )

        return queryset.count()

    @cache_response(key_func=duns_count_key_function)
    def post(self, request):
        validated_payload = TinyShield(self.models).block(request.data)
        return Response({"count": self.get_count(validated_payload)})
