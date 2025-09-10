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

class RecipientPOPStrategy(ABC):

    def get_query(self, filter_type: str, filter_values, query_type: QueryType) -> ES_Q:
        match filter_type:
            case "recipient_search_text":
                return _RecipientSearchText.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type)
            case "recipient_id":
                return _RecipientId.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type)
            case "recipient_scope":
                return _RecipientPOPScope.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type, receiver="recipient")
            case "place_of_performance_scope":
                return _RecipientPOPScope.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type, receiver="pop")
            case "recipient_locations":
                return _RecipientPOPLocations.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type, receiver="recipient_location")
            case "place_of_performance_locations":
                return _RecipientPOPLocations.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type, receiver="pop")
            case _:
                return InvalidParameterException(f"Invalid filter: {filter_type} does not exist.")



class _RecipientSearchText():
    @staticmethod
    def generate_elasticsearch_query(filter_values: List[str], query_type: QueryType, ** options) -> ES_Q:
        recipient_search_query = []
        words_to_escape = ["AND", "OR"]  # These need to be escaped to be included as text to be searched for

        for filter_value in filter_values:

            parent_recipient_unique_id_field = None
            parent_uei_field = None

            if query_type == QueryType.SUBAWARDS:
                fields = ["sub_awardee_or_recipient_legal"]
                upper_recipient_string = es_sanitize(filter_value.upper())
                query = es_sanitize(upper_recipient_string)
                recipient_unique_id_field = "sub_awardee_or_recipient_uniqu"
                recipient_uei_field = "sub_awardee_or_recipient_uei"
            else:
                fields = ["recipient_name", "parent_recipient_name"]
                upper_recipient_string = es_sanitize(filter_value.upper())
                query = es_sanitize(upper_recipient_string) + "*"
                if "\\" in es_sanitize(upper_recipient_string):
                    query = es_sanitize(upper_recipient_string) + r"\*"
                recipient_unique_id_field = "recipient_unique_id"
                recipient_uei_field = "recipient_uei"
                parent_recipient_unique_id_field = "parent_recipient_unique_id"
                parent_uei_field = "parent_uei"

            for special_word in words_to_escape:
                if len(re.findall(rf"\b{special_word}\b", query)) > 0:
                    query = re.sub(rf"\b{special_word}\b", rf"\\{special_word}", query)
            recipient_name_query = ES_Q("query_string", query=query, default_operator="AND", fields=fields)

            if len(upper_recipient_string) == 9 and upper_recipient_string[:5].isnumeric():
                recipient_duns_query = ES_Q("match", **{recipient_unique_id_field: upper_recipient_string})
                recipient_search_query.append(ES_Q("dis_max", queries=[recipient_name_query, recipient_duns_query]))
                if parent_recipient_unique_id_field is not None:
                    parent_recipient_duns_query = ES_Q(
                        "match", **{parent_recipient_unique_id_field: upper_recipient_string}
                    )
                    recipient_search_query.append(
                        ES_Q("dis_max", queries=[recipient_name_query, parent_recipient_duns_query])
                    )
                else:
                    recipient_search_query.append(ES_Q("dis_max", queries=[recipient_name_query]))
            if len(upper_recipient_string) == 12:
                recipient_uei_query = ES_Q("match", **{recipient_uei_field: upper_recipient_string})
                recipient_search_query.append(ES_Q("dis_max", queries=[recipient_name_query, recipient_uei_query]))
                if parent_uei_field is not None:
                    parent_recipient_uei_query = ES_Q("match", **{parent_uei_field: upper_recipient_string})
                    recipient_search_query.append(
                        ES_Q("dis_max", queries=[recipient_name_query, parent_recipient_uei_query])
                    )
                else:
                    recipient_search_query.append(ES_Q("dis_max", queries=[recipient_name_query]))
            # If the recipient name ends with a period, then add a regex query to find results ending with a
            #   period and results with a period in the same location but with characters following it.
            # Example: A query for COMPANY INC. will return both COMPANY INC. and COMPANY INC.XYZ
            if upper_recipient_string.endswith(".") and query_type != QueryType.SUBAWARDS:
                recipient_search_query.append(recipient_name_query)
                recipient_search_query.append(
                    ES_Q({"regexp": {"recipient_name.keyword": f"{upper_recipient_string.rstrip('.')}\\..*"}})
                )
            else:
                recipient_search_query.append(recipient_name_query)

        return ES_Q("bool", should=recipient_search_query, minimum_should_match=1)

class _RecipientId(RecipientPOPStrategy):
    underscore_name = "recipient_id"

    @staticmethod
    def generate_elasticsearch_query(filter_value: str, query_type: QueryType, **options) -> ES_Q:
        recipient_hash = filter_value[:-2]
        if query_type == QueryType.SUBAWARDS:
            # Subawards did not support "recipient_id" before migrating to elastic search
            # so this behavior is honored here.
            raise InvalidParameterException(
                f"Invalid filter: {_RecipientId.underscore_name} is not supported for subaward queries."
            )
        if filter_value.endswith("P"):
            return ES_Q("match", parent_recipient_hash=recipient_hash)
        elif filter_value.endswith("C"):
            return ES_Q("match", recipient_hash=recipient_hash) & ~ES_Q("match", parent_uei__keyword="NULL")
        else:
            return ES_Q("match", recipient_hash=recipient_hash) & ~ES_Q("exists", field="parent_uei")


class _RecipientTypeNames(RecipientPOPStrategy):
    underscore_name = "recipient_type_names"

    @staticmethod
    def generate_elasticsearch_query(filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
        recipient_type_query = []

        for filter_value in filter_values:
            recipient_type_query.append(ES_Q("match", business_categories=filter_value))

        return ES_Q("bool", should=recipient_type_query, minimum_should_match=1)

class _RecipientPOPLocations(RecipientPOPStrategy):
    @staticmethod
    def generate_elasticsearch_query(filter_values, query_type: QueryType, receiver: str, **options) -> ES_Q:
        recipient_locations_query = []
        field_prefix = "sub_" if query_type == QueryType.SUBAWARDS else ""
        for filter_value in filter_values:
            county = filter_value.get("county")
            state = filter_value.get("state")
            country = filter_value.get("country")
            district_current = filter_value.get("district_current")
            district_original = filter_value.get("district_original")
            location_lookup = {
                "country_code": country,
                "state_code": state,
                "county_code": county,
                "congressional_code_current": district_current,
                "congressional_code": district_original,
                "city_name__keyword": filter_value.get("city"),
                "zip5": filter_value.get("zip"),
            }

            if (state is None or country != "USA" or county is not None) and (
                district_current is not None or district_original is not None
            ):
                raise InvalidParameterException(INCOMPATIBLE_DISTRICT_LOCATION_PARAMETERS)

            location_query = [
                ES_Q("match", **{f"{field_prefix}{receiver}_{location_key}": location_value.upper()})
                for location_key, location_value in location_lookup.items()
                if location_value is not None
            ]

            if location_query:
                recipient_locations_query.append(ES_Q("bool", must=location_query))

        return ES_Q("bool", should=recipient_locations_query, minimum_should_match=1)

class _RecipientPOPScope(RecipientPOPStrategy):

    @classmethod
    def generate_elasticsearch_query(cls, filter_value: str, query_type: QueryType, receiver: str, **options) -> ES_Q:
        if query_type == QueryType.SUBAWARDS:
            recipient_scope_query = ES_Q("match", **{f"sub_{receiver}_location_country_code": "USA"}) | ES_Q(
                "match", **{f"sub_{receiver}_location_country_name": "UNITED STATES"}
            )
        else:
            recipient_scope_query = ES_Q("match", **{f"{receiver}_location_country_code=": "USA"})

        if filter_value == "domestic":
            return recipient_scope_query
        elif filter_value == "foreign":
            return ~recipient_scope_query