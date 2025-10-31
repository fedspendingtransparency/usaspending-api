import copy
import logging
from decimal import Decimal

from django.conf import settings
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.api_versioning import API_TRANSFORM_FUNCTIONS, api_transformations
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.helpers.generic_helper import get_generic_filters_message, under_development_message
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import (
    AWARD_FILTER,
    AWARD_FILTER_NO_RECIPIENT_ID,
)
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.search.v2.es_sanitization import es_minimal_sanitize

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
        if "keywords" in validated_payload["filters"]:
            validated_payload["filters"]["keyword_search"] = [
                es_minimal_sanitize(keyword) for keyword in validated_payload["filters"]["keywords"]
            ]
            validated_payload["filters"].pop("keywords")

        # <Field in API request> : <Field name in ElasticSearch document>
        api_request_field_to_es_field_mapper = {
            "award_generated_internal_id": "generated_unique_award_id",
            "award_id": "display_award_id",
            "transaction_count": "transaction_count",
            "transaction_obligation": "generated_pragmatic_obligation",
        }

        query_with_filters = QueryWithFilters(QueryType.AWARDS)
        filter_query = query_with_filters.generate_elasticsearch_query(validated_payload["filters"])
        search = (
            AwardSearch()
            .filter(filter_query)
            .source(fields=list(api_request_field_to_es_field_mapper.values()))
            .sort(
                {
                    api_request_field_to_es_field_mapper[self.sort_by]: {
                        "order": "asc" if validated_payload["order"] == "asc" else "desc"
                    }
                }
            )
            .extra(from_=lower_limit, size=validated_payload["limit"])
        )
        es_response = search.handle_execute()

        results = [
            {
                "award_id": source["_source"]["display_award_id"],
                "transaction_count": source["_source"]["transaction_count"],
                "transaction_obligation": (
                    Decimal(source["_source"]["generated_pragmatic_obligation"]).quantize(Decimal(".01"))
                    if source["_source"]["generated_pragmatic_obligation"]
                    else 0.0
                ),
                "award_generated_internal_id": source["_source"]["generated_unique_award_id"],
            }
            for source in es_response.hits.hits
        ]

        has_next = es_response.hits.total.value > (validated_payload["limit"] * validated_payload["page"])
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
                "results": results,
                "page_metadata": metadata,
                "messages": [
                    under_development_message(),
                    *get_generic_filters_message(
                        validated_payload["filters"].keys(), [elem["name"] for elem in AWARD_FILTER_NO_RECIPIENT_ID]
                    ),
                ],
            }
        )
