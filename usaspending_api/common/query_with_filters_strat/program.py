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


class ProgramStrategy(ABC):
    def get_query(self, filter_type: str, filter_values, query_type: QueryType) -> ES_Q:
        match filter_type:
            case "program_activities":
                return _ProgramActivities.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type)
            case "program_numbers":
                return _ProgramNumbers.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type)



class _ProgramActivities(ABC):
    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[dict], query_type: QueryType, **options) -> ES_Q:
        program_activity_match_queries = []

        for filter_value in filter_values:
            temp_must = []

            if "name" in filter_value:
                temp_must.append(
                    ES_Q("match", program_activities__name__keyword=filter_value["name"].upper()),
                )
            if "code" in filter_value:
                temp_must.append(ES_Q("match", program_activities__code__keyword=str(filter_value["code"]).zfill(4)))

            if "type" in filter_value:
                temp_must.append(ES_Q("match", program_activities__type__keyword=filter_value["type"].upper()))

            if temp_must:
                program_activity_match_queries.append(
                    ES_Q("nested", path="program_activities", query=ES_Q("bool", must=temp_must))
                )

        if len(program_activity_match_queries) == 0:
            return ~ES_Q()
        return ES_Q("bool", should=program_activity_match_queries, minimum_should_match=1)


class _ProgramNumbers(ABC):

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
        programs_numbers_query = []

        for filter_value in filter_values:
            if query_type == QueryType.AWARDS:
                escaped_program_number = filter_value.replace(".", "\\.")
                r = f""".*\\"cfda_number\\" *: *\\"{escaped_program_number}\\".*"""
                programs_numbers_query.append(ES_Q("regexp", cfdas=r))
            else:
                programs_numbers_query.append(ES_Q("match", cfda_number=filter_value))

        return ES_Q("bool", should=programs_numbers_query, minimum_should_match=1)
