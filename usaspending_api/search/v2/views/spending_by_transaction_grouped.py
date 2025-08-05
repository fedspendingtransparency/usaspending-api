import copy
import logging
from decimal import Decimal

from django.conf import settings
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.helpers.generic_helper import get_generic_filters_message
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.common.validator.award_filter import (
    AWARD_FILTER,
    AWARD_FILTER_NO_RECIPIENT_ID,
)
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.v2.elasticsearch_helper import get_number_of_unique_terms_for_transactions
from usaspending_api.search.v2.es_sanitization import es_minimal_sanitize
from elasticsearch_dsl import A

logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByTransactionGroupedVisualizationViewSet(APIView):
    """
    This route provides aggregated information about transactions grouped by their prime awards. Additionally, allows
    the transactions to be filtered.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_transaction_grouped.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        models = [
            {
                "name": "sort",
                "key": "sort",
                "type": "enum",
                "enum_values": (
                    "award_generated_internal_id",
                    "award_id",
                    "transaction_count",
                    "transaction_obligation",
                ),
                "optional": True,
                "default": "transaction_obligation",
            },
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy([model for model in PAGINATION if model["name"] != "sort"]))
        for m in models:
            if m["name"] == "award_type_codes":
                m["optional"] = False
        self.tinysheld = TinyShield(models)
        self.sort_by: str | None = None

    @cache_response()
    def post(self, request: Request) -> Response:
        validated_payload = self.tinysheld.block(request.data)
        self.sort_by = validated_payload["sort"]
        record_num = (validated_payload["page"] - 1) * validated_payload["limit"]
        if record_num >= settings.ES_TRANSACTIONS_MAX_RESULT_WINDOW:
            raise UnprocessableEntityException(
                "Page #{page} of size {limit} is over the maximum result limit ({es_limit}). Consider using custom data downloads to obtain large data sets.".format(
                    page=validated_payload["page"],
                    limit=validated_payload["limit"],
                    es_limit=settings.ES_TRANSACTIONS_MAX_RESULT_WINDOW,
                )
            )
        if "filters" in validated_payload and "no intersection" in validated_payload["filters"]["award_type_codes"]:
            # "Special case": there will never be results when the website provides this value
            return Response(
                {
                    "limit": validated_payload["limit"],
                    "results": [],
                    "page_metadata": {
                        "page": validated_payload["page"],
                        "next": None,
                        "previous": None,
                        "hasNext": False,
                        "hasPrevious": False,
                    },
                }
            )
        lower_limit = (validated_payload["page"] - 1) * validated_payload["limit"]
        upper_limit = (validated_payload["page"]) * validated_payload["limit"] + 1
        if "keywords" in validated_payload["filters"]:
            validated_payload["filters"]["keyword_search"] = [
                es_minimal_sanitize(keyword) for keyword in validated_payload["filters"]["keywords"]
            ]
            validated_payload["filters"].pop("keywords")
        query_with_filters = QueryWithFilters(QueryType.TRANSACTIONS)
        filter_query = query_with_filters.generate_elasticsearch_query(validated_payload["filters"])
        search = TransactionSearch().filter(filter_query)
        (
            search.aggs.bucket("group_by_prime_award", A("terms", field="display_award_id"))
            .metric("transaction_obligation", A("sum", field="federal_action_obligation"))
            .metric("award_generated_internal_id", A("terms", field="generated_unique_award_id"))
        )
        bucket_count = get_number_of_unique_terms_for_transactions(filter_query, "display_award_id")
        agg_response = search.handle_execute()

        agg_buckets = sorted(
            agg_response.aggregations.group_by_prime_award.buckets,
            key=self.sort_fn,
            reverse=True if validated_payload["order"] == "desc" else False,
        )[lower_limit:upper_limit]
        results = [
            {
                "award_id": prime_award.key,
                "transaction_count": prime_award.doc_count,
                "transaction_obligation": Decimal(prime_award.transaction_obligation.value).quantize(Decimal(".01")),
                "award_generated_internal_id": prime_award.award_generated_internal_id.buckets[0].key,
            }
            for prime_award in agg_buckets
        ]
        has_next = bucket_count > validated_payload["limit"]
        has_previous = validated_payload["page"] > 1
        metadata = {
            "page": validated_payload["page"],
            "next": validated_payload["page"] + 1 if has_next else None,
            "previous": validated_payload["page"] - 1 if has_previous else None,
            "hasNext": has_next,
            "hasPrevious": has_previous,
        }
        return Response(
            {
                "limit": validated_payload["limit"],
                "results": results[: validated_payload["limit"]],
                "page_metadata": metadata,
                "messages": get_generic_filters_message(
                    validated_payload["filters"].keys(), [elem["name"] for elem in AWARD_FILTER_NO_RECIPIENT_ID]
                ),
            }
        )

    def sort_fn(self, item):
        match self.sort_by:
            case "award_id":
                key = item.key
            case "transaction_count":
                key = item.doc_count
            case "award_generated_internal_id":
                key = item.award_generated_internal_id.buckets[0].key
            case "transaction_obligation":
                key = item.transaction_obligation.value
            case _:
                raise ValueError("Invalid sort key")
        return key
