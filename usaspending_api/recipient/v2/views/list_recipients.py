import logging
import copy

from rest_framework.response import Response
from django.db.models import F, Q

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.common.validator.utils import update_model_in_list
from usaspending_api.recipient.models import RecipientProfile
from usaspending_api.recipient.v2.lookups import SPECIAL_CASES

logger = logging.getLogger(__name__)


# based from all_award_types_mappings in usaspending_api.awards.v2.lookups.lookups
AWARD_TYPES = {
    "contracts": {"amount": "last_12_contracts", "filter": "contract"},
    "grants": {"amount": "last_12_grants", "filter": "grant"},
    "direct_payments": {"amount": "last_12_direct_payments", "filter": "direct payment"},
    "loans": {"amount": "last_12_loans", "filter": "loans"},
    "other_financial_assistance": {"amount": "last_12_other", "filter": "other"},
}


def get_recipients(filters={}):
    lower_limit = (filters["page"] - 1) * filters["limit"]
    upper_limit = filters["page"] * filters["limit"]

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

    api_to_db_mapper = {"amount": amount_column, "duns": "recipient_unique_id", "name": "recipient_name"}

    if filters["order"] == "desc":
        queryset = queryset.order_by(F(api_to_db_mapper[filters["sort"]]).desc(nulls_last=True))
    else:
        queryset = queryset.order_by(F(api_to_db_mapper[filters["sort"]]).asc(nulls_last=True))

    count = queryset.count()
    page_metadata = get_pagination_metadata(count, filters["limit"], filters["page"])

    results = [
        {
            "id": "{}-{}".format(row["recipient_hash"], row["recipient_level"]),
            "duns": row["recipient_unique_id"],
            "name": row["recipient_name"],
            "recipient_level": row["recipient_level"],
            "amount": row[amount_column],
        }
        for row in queryset[lower_limit:upper_limit]
    ]

    return results, page_metadata


class ListRecipients(APIDocumentationView):
    """
    This route takes a single keyword filter (and pagination filters), and returns a list of recipients
    endpoint_doc: /recipient/list_recipients.md
    """

    @cache_response()
    def post(self, request):
        award_types = list(AWARD_TYPES.keys()) + ["all"]
        models = [
            {"name": "keyword", "key": "keyword", "type": "text", "text_type": "search"},
            {"name": "award_type", "key": "award_type", "type": "enum", "enum_values": award_types, "default": "all"},
        ]
        models.extend(copy.deepcopy(PAGINATION))  # page, limit, sort, order

        new_sort = {"type": "enum", "enum_values": ["name", "duns", "amount"], "default": "amount"}
        models = update_model_in_list(models, "sort", new_sort)
        models = update_model_in_list(models, "limit", {"default": 50})
        validated_payload = TinyShield(models).block(request.data)

        results, page_metadata = get_recipients(filters=validated_payload)
        return Response({"page_metadata": page_metadata, "results": results})
