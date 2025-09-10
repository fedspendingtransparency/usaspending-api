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

class TypeCodesStrategy(ABC):

    def get_query(self, filter_type: str, filter_values: List[str], query_type: QueryType) -> ES_Q:

        match filter_type:
            case "contract_pricing_type_codes":
                return _TypeCodes.generate_elasticsearch_query(filter_values=filter_values, type_code="type_of_contract_pricing__keyword")
            case "set_aside_type_codes":
                return _TypeCodes.generate_elasticsearch_query(filter_values=filter_values, type_code="type_set_aside__keyword")
            case "extent_competed_type_codes":
                return _TypeCodes.generate_elasticsearch_query(filter_values=filter_values, type_code="extent_competed__keyword")
            case "award_type_codes":
                if query_type == QueryType.SUBAWARDS:
                    return _TypeCodes.generate_elasticsearch_query(filter_values=filter_values, type_code="prime_award_type")
                return _TypeCodes.generate_elasticsearch_query(filter_values=filter_values, type_code="type")
            case _:
                raise InvalidParameterException(f"Invalid filter: {filter_type} does not exist.")


class _TypeCodes(TypeCodesStrategy):

    @staticmethod
    def generate_elasticsearch_query(filter_values: List[str], type_code: str, **options) -> ES_Q:
        type_code_query = []

        for filter_value in filter_values:
            type_code_query.append(ES_Q("match", **{type_code: filter_value}))

        return ES_Q("bool", should=type_code_query, minimum_should_match=1)

