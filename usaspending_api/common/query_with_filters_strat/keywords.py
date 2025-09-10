from abc import ABC, abstractmethod
import re
from typing import List
import itertools
import logging
from elasticsearch_dsl import Q as ES_Q
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.references.models.psc import PSC

from usaspending_api.search.v2.es_sanitization import es_sanitize
from usaspending_api.common.helpers.api_helper import (
    INCOMPATIBLE_DISTRICT_LOCATION_PARAMETERS,
)

logger = logging.getLogger(__name__)

class KeywordStrategy(ABC):
    def get_query(self, filter_type: str, filter_values, query_type: QueryType):
        match filter_type:
            case "keywords":
                if query_type == QueryType.SUBAWARDS:
                    return _SubawardsKeyword.generate_elasticsearch_query(filter_values=filter_type, query_type=query_type)
                return _Keywords.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type)
            case "transaction_keyword_search":
                return _TransactionKeywordSearch.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type)
            case "keyword_search":
                return _KeywordSearch.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type)
            case _:
                raise InvalidParameterException(f"Invalid filter: {filter_type} does not exist.")


class _KeywordSearch(ABC):
    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
        keyword_queries = []
        keyword_fields = [
            "recipient_location_congressional_code",
            "recipient_location_county_code",
            "recipient_location_country_code",
            "recipient_location_state_code",
            "business_categories",
            "pop_congressional_code",
            "pop_country_code",
            "pop_state_code",
            "pop_county_code",
            "cfda_number",
            "fain",
            "piid",
            "uri",
        ]
        text_fields = [
            "recipient_name",
            "parent_recipient_name",
            "naics_code",
            "naics_description",
            "product_or_service_code",
            "product_or_service_description",
            "transaction_description",
            "recipient_unique_id",
            "parent_recipient_unique_id",
            "description",
            "award_description",
            "cfda_title",
            "awarding_toptier_agency_name",
            "awarding_subtier_agency_name",
            "funding_toptier_agency_name",
            "funding_subtier_agency_name",
            "type_description",
            "pop_country_name",
            "pop_county_name",
            "pop_zip5",
            "pop_city_name",
            "recipient_location_country_name",
            "recipient_location_county_name",
            "recipient_location_zip5",
            "recipient_location_city_name",
            "modification_number",
            "recipient_uei",
            "parent_uei",
            "sub_awardee_or_recipient_uniqu",
            "product_or_service_code",
            "sub_awardee_or_recipient_uei",
            "sub_ultimate_parent_unique_ide",
            "sub_ultimate_parent_uei",
        ]
        for filter_value in filter_values:
            keyword_queries.append(ES_Q("multi_match", query=filter_value, fields=text_fields, type="phrase_prefix"))
            keyword_queries.append(
                ES_Q("query_string", query=filter_value, default_operator="OR", fields=keyword_fields)
            )

        return ES_Q("dis_max", queries=keyword_queries)

class _Keywords(ABC):
    underscore_name = "keywords"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
        keyword_queries = []
        keyword_fields = [
            "piid",
            "fain",
            "uri",
        ]
        text_fields = [
            "recipient_name",
            "naics_description",
            "product_or_service_description",
            "transaction_description",
            "recipient_unique_id",
            "parent_recipient_unique_id",
            "description",
            "recipient_uei",
            "parent_uei",
            "sub_awardee_or_recipient_uniqu",
            "product_or_service_code",
            "sub_awardee_or_recipient_uei",
            "sub_ultimate_parent_unique_ide",
            "sub_ultimate_parent_uei",
        ]
        for filter_value in filter_values:
            query = es_sanitize(filter_value)
            if query_type != QueryType.SUBAWARDS:
                query = query + "*"
                if "\\" in es_sanitize(filter_value):
                    query = es_sanitize(filter_value) + r"\*"
            else:
                query = query.upper()

            keyword_queries.append(ES_Q("query_string", query=query, default_operator="AND", fields=keyword_fields))
            keyword_queries.append(ES_Q("multi_match", query=query, fields=text_fields, type="phrase_prefix"))

        return ES_Q("dis_max", queries=keyword_queries)

class _TransactionKeywordSearch(ABC):
    """Intended for subawards' and awards' Querytype that makes this query compatible with Subawards and Awards."""

    @staticmethod
    def generate_elasticsearch_query(filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
        from usaspending_api.search.v2 import elasticsearch_helper

        transaction_id_queries = []
        for keyword in filter_values:
            transaction_ids = elasticsearch_helper.get_download_ids(keyword=keyword, field="transaction_id")
            transaction_ids = list(itertools.chain.from_iterable(transaction_ids))
            logger.info("Found {} transactions based on keyword: {}".format(len(transaction_ids), keyword))
            for transaction_id in transaction_ids:
                transaction_id_queries.append(
                    ES_Q("match", latest_transaction_id=str(transaction_id), minimum_should_match=1)
                )

        return ES_Q("bool", must=transaction_id_queries) & ~ES_Q("match", latest_transaction__keyword="NULL")

class _SubawardsKeyword(ABC):
    @staticmethod
    def generate_elasticsearch_query(filter_values, query_type: QueryType, **options) -> ES_Q:
        keyword_queries = []

        def keyword_parse(keyword):
            fields = [
                "sub_awardee_or_recipient_legal",
                "product_or_service_description",
                "subaward_description",
                "subaward_number",
            ]
            queries = [
                ES_Q("multi_match", query=keyword, fields=fields, type="phrase_prefix"),
                ES_Q("match", award_piid_fain=keyword),
            ]
            if len(keyword) == 4 and PSC.objects.filter(code=keyword).exists():
                queries.append(ES_Q("match", product_or_service_code=keyword))
            return ES_Q("bool", should=queries, minimum_should_match=1)

        keyword_queries = []
        for keyword in filter_values:
            curr_queries = []
            curr_queries.append(keyword_parse(keyword))

            # Search for DUNS
            potential_duns = keyword if len(keyword) == 9 else None
            potential_duns_queries = []
            if potential_duns is not None:
                potential_duns_queries.append(ES_Q("match", sub_awardee_or_recipient_uniqu=potential_duns))
                potential_duns_queries.append(ES_Q("match", sub_ultimate_parent_unique_ide=potential_duns))
                dun_query = ES_Q("bool", should=potential_duns_queries, minimum_should_match=1)
                curr_queries.append(dun_query)

            # Search for UEI
            potential_uei = keyword.upper() if len(keyword) == 12 else None
            if potential_uei is not None:
                uei_query = ES_Q("match", sub_awardee_or_recipient_uei=potential_uei) | ES_Q(
                    "match", sub_ultimate_parent_uei=potential_uei
                )
                curr_queries.append(uei_query)
            curr_query = ES_Q("bool", should=curr_queries, minimum_should_match=1)
            keyword_queries.append(curr_query)

        return ES_Q("dis_max", queries=keyword_queries)