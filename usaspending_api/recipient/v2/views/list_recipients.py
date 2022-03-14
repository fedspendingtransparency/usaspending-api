import copy
import logging

from django.db.models import F, Q
from django.utils.decorators import method_decorator
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.api_versioning import deprecated
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.common.validator.utils import update_model_in_list
from usaspending_api.recipient.models import RecipientProfile
from usaspending_api.recipient.v2.lookups import AWARD_TYPES, SPECIAL_CASES

logger = logging.getLogger(__name__)

award_types = list(AWARD_TYPES.keys()) + ["all"]
RECIPIENT_MODELS = [
    {"name": "keyword", "key": "keyword", "type": "text", "text_type": "search"},
    {"name": "award_type", "key": "award_type", "type": "enum", "enum_values": award_types, "default": "all"},
]


def build_recipient_identifier_base_query(filters):
    qs_filter = Q()
    if "keyword" in filters:
        qs_filter |= Q(recipient_name__contains=filters["keyword"].upper())
        qs_filter |= Q(recipient_unique_id__contains=filters["keyword"]) | Q(uei__contains=filters["keyword"])

    if filters["award_type"] != "all":
        qs_filter &= Q(award_types__overlap=[AWARD_TYPES[filters["award_type"]]["filter"]])

    return qs_filter


def get_recipients(filters={}, count=None):
    lower_limit = (filters["page"] - 1) * filters["limit"]
    upper_limit = filters["page"] * filters["limit"]

    amount_column = "last_12_months"
    if filters["award_type"] != "all":
        amount_column = AWARD_TYPES[filters["award_type"]]["amount"]

    qs_filter = build_recipient_identifier_base_query(filters)

    queryset = (
        RecipientProfile.objects.filter(qs_filter)
        .values("recipient_level", "recipient_hash", "recipient_unique_id", "recipient_name", amount_column, "uei")
        .exclude(recipient_name__in=SPECIAL_CASES)
    )
    api_to_db_mapper = {
        "amount": amount_column,
        "duns": "recipient_unique_id",
        "uei": "uei",
        "name": "recipient_name",
    }

    # Nulls Last isn't enabled for the amount sort because it prevents queries sorted by amount columns DESC
    # from using an index on those columns, even though they cannot contain nulls
    nulls_last = filters["sort"] in ["name", "duns"]

    if filters["order"] == "desc":
        queryset = queryset.order_by(F(api_to_db_mapper[filters["sort"]]).desc(nulls_last=nulls_last))
    else:
        queryset = queryset.order_by(F(api_to_db_mapper[filters["sort"]]).asc(nulls_last=nulls_last))

    if count is None:
        count = get_recipient_count(filters=filters)

    page_metadata = get_pagination_metadata(count, filters["limit"], filters["page"])

    results = [
        {
            "id": "{}-{}".format(row["recipient_hash"], row["recipient_level"]),
            "duns": row["recipient_unique_id"],
            "uei": row["uei"],
            "name": row["recipient_name"],
            "recipient_level": row["recipient_level"],
            "amount": row[amount_column],
        }
        for row in queryset[lower_limit:upper_limit]
    ]

    return results, page_metadata


def get_recipient_count(filters={}):
    qs_filter = build_recipient_identifier_base_query(filters)
    return RecipientProfile.objects.filter(qs_filter).exclude(recipient_name__in=SPECIAL_CASES).count()


class RecipientCount(APIView):
    """
    This route takes a single keyword filter and award_type, and returns a count of matching recipients
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/recipient/count.md"

    cache_key_whitelist = ["keyword", "award_type"]

    @cache_response()
    def post(self, request):
        validated_payload = TinyShield(RECIPIENT_MODELS).block(request.data)
        return Response({"count": get_recipient_count(validated_payload)})


class ListRecipients(APIView):
    """
    This route takes a single keyword filter (and pagination filters), and returns a list of recipients
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/recipient.md"

    def request_count(self, filters={}):
        response = RecipientCount.as_view()(request=self.request._request).data
        return response["count"]

    @cache_response()
    def post(self, request):
        models = copy.deepcopy(RECIPIENT_MODELS)
        models.extend(copy.deepcopy(PAGINATION))  # page, limit, sort, order

        # Override pagination default limit of 100
        for model in models:
            if model["name"] == "limit":
                model["max"] = 1000

        new_sort = {"type": "enum", "enum_values": ["name", "uei", "duns", "amount"], "default": "amount"}
        models = update_model_in_list(models, "sort", new_sort)
        models = update_model_in_list(models, "limit", {"default": 50})
        validated_payload = TinyShield(models).block(request.data)

        count = self.request_count(validated_payload)

        results, page_metadata = get_recipients(filters=validated_payload, count=count)
        return Response({"page_metadata": page_metadata, "results": results})


@method_decorator(deprecated, name="post")
class ListRecipientsByDuns(ListRecipients):
    """
    <em>Deprecated: Please see <a href="../">this endpoint</a> instead.</em>

    This route takes a single keyword filter (and pagination filters), and returns a list of recipients
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/recipient/duns.md"

    def __init__(self):
        super().__init__()
