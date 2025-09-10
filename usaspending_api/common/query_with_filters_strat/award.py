from abc import ABC, abstractmethod
import re
from typing import List

from elasticsearch_dsl import Q as ES_Q
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.search.v2.es_sanitization import es_sanitize
from usaspending_api.common.helpers.api_helper import (
    INCOMPATIBLE_DISTRICT_LOCATION_PARAMETERS,
)

class AwardStrategy(ABC):

    def get_query(self, filter_type: str, filter_values, query_type: QueryType):
        match filter_type:
            case "award_ids":
                return _AwardIds.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type)
            case "award_amounts":
                return _AwardAmounts.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type)
            case "award_unique_id":
                return _AwardUniqueId.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type)


class _AwardIds(ABC):

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
        award_ids_query = []

        if query_type == QueryType.SUBAWARDS:
            award_id_fields = ["award_piid_fain", "subaward_number"]
        else:
            award_id_fields = ["display_award_id"]

        for filter_value in filter_values:
            if filter_value and filter_value.startswith('"') and filter_value.endswith('"'):
                filter_value = filter_value[1:-1]
                award_ids_query.extend(
                    ES_Q("term", **{es_field: {"query": es_sanitize(filter_value)}}) for es_field in award_id_fields
                )
            else:
                filter_value = es_sanitize(filter_value)
                filter_value = " +".join(filter_value.split())
                award_ids_query.extend(
                    ES_Q("regexp", **{es_field: {"value": es_sanitize(filter_value)}}) for es_field in award_id_fields
                )

        return ES_Q("bool", should=award_ids_query, minimum_should_match=1)

class _AwardAmounts(ABC):
    underscore_name = "award_amounts"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[dict], query_type: QueryType, **options) -> ES_Q:
        award_amounts_query = []
        if query_type == QueryType.SUBAWARDS:
            filter_field = "subaward_amount"
        else:
            filter_field = "award_amount"

        for filter_value in filter_values:
            lower_bound = filter_value.get("lower_bound")
            upper_bound = filter_value.get("upper_bound")
            award_amounts_query.append(ES_Q("range", **{filter_field: {"gte": lower_bound, "lte": upper_bound}}))
        return ES_Q("bool", should=award_amounts_query, minimum_should_match=1)

class _AwardUniqueId(ABC):
    """String that represents the unique ID of the prime award of a queried award/subaward."""

    underscore_name = "award_unique_id"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: str, query_type: QueryType, **options) -> ES_Q:

        fields = {
            QueryType.AWARDS: ["generated_unique_award_id"],
            QueryType.SUBAWARDS: ["unique_award_key"],
            QueryType.TRANSACTIONS: ["generated_unique_award_id"],
        }

        query = es_sanitize(filter_values)
        id_query = ES_Q("query_string", query=query, default_operator="AND", fields=fields.get(query_type, []))
        return ES_Q("bool", should=id_query, minimum_should_match=1)