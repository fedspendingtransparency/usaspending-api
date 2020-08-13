import logging

from django.conf import settings
from elasticsearch_dsl import Q as ES_Q
from typing import List
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.search.filters.elasticsearch.filter import _Filter, _QueryType
from usaspending_api.search.filters.elasticsearch.naics import NaicsCodes
from usaspending_api.search.filters.elasticsearch.psc import PSCCodes
from usaspending_api.search.filters.elasticsearch.tas import TasCodes, TreasuryAccounts
from usaspending_api.search.v2.es_sanitization import es_sanitize


logger = logging.getLogger(__name__)


class _Keywords(_Filter):
    underscore_name = "keywords"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: _QueryType) -> ES_Q:
        keyword_queries = []
        fields = [
            "recipient_name",
            "naics_description",
            "product_or_service_description",
            "award_description",
            "piid",
            "fain",
            "uri",
            "recipient_unique_id",
            "parent_recipient_unique_id",
            "description",
        ]
        for v in filter_values:
            query = es_sanitize(v) + "*"
            if "\\" in es_sanitize(v):
                query = es_sanitize(v) + r"\*"
            keyword_queries.append(ES_Q("query_string", query=query, default_operator="AND", fields=fields))

        return ES_Q("dis_max", queries=keyword_queries)


class _KeywordSearch(_Filter):
    underscore_name = "keyword_search"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: _QueryType) -> ES_Q:
        keyword_queries = []
        fields = [
            "recipient_name",
            "parent_recipient_name",
            "naics_code",
            "naics_description",
            "product_or_service_code",
            "product_or_service_description",
            "award_description",
            "piid",
            "fain",
            "uri",
            "recipient_unique_id",
            "parent_recipient_unique_id",
            "description",
            "cfda_number",
            "cfda_title",
            "awarding_toptier_agency_name",
            "awarding_subtier_agency_name",
            "funding_toptier_agency_name",
            "funding_subtier_agency_name",
            "business_categories",
            "type_description",
            "pop_country_code",
            "pop_country_name",
            "pop_state_code",
            "pop_county_code",
            "pop_county_name",
            "pop_zip5",
            "pop_congressional_code",
            "pop_city_name",
            "recipient_location_country_code",
            "recipient_location_country_name",
            "recipient_location_state_code",
            "recipient_location_county_code",
            "recipient_location_county_name",
            "recipient_location_zip5",
            "recipient_location_congressional_code",
            "recipient_location_city_name",
            "modification_number",
        ]
        for v in filter_values:
            keyword_queries.append(ES_Q("query_string", query=v, default_operator="OR", fields=fields))

        return ES_Q("dis_max", queries=keyword_queries)


class _TimePeriods(_Filter):
    underscore_name = "time_period"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[dict], query_type: _QueryType) -> ES_Q:
        time_period_query = []

        for v in filter_values:
            start_date = v.get("start_date") or settings.API_SEARCH_MIN_DATE
            end_date = v.get("end_date") or settings.API_MAX_DATE
            if query_type == _QueryType.AWARDS:
                time_period_query.append(
                    ES_Q(
                        "bool",
                        should=[
                            ES_Q("range", action_date={"gte": start_date}),
                            ES_Q("range", date_signed={"lte": end_date}),
                        ],
                        minimum_should_match=2,
                    )
                )

            else:
                time_period_query.append(ES_Q("range", action_date={"gte": start_date, "lte": end_date}))

        return ES_Q("bool", should=time_period_query, minimum_should_match=1)


class _AwardTypeCodes(_Filter):
    underscore_name = "award_type_codes"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: _QueryType) -> ES_Q:
        award_type_codes_query = []

        for v in filter_values:
            award_type_codes_query.append(ES_Q("match", type=v))

        return ES_Q("bool", should=award_type_codes_query, minimum_should_match=1)


class _Agencies(_Filter):
    underscore_name = "agencies"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[dict], query_type: _QueryType) -> List[ES_Q]:
        awarding_agency_query = []
        funding_agency_query = []

        for v in filter_values:
            agency_name = v["name"]
            agency_tier = v["tier"]
            agency_type = v["type"]
            toptier_name = v.get("toptier_name")
            agency_query = ES_Q("match", **{f"{agency_type}_{agency_tier}_agency_name__keyword": agency_name})
            if agency_tier == "subtier" and toptier_name is not None:
                agency_query &= ES_Q("match", **{f"{agency_type}_toptier_agency_name__keyword": toptier_name})
            if agency_type == "awarding":
                awarding_agency_query.append(agency_query)
            elif agency_type == "funding":
                funding_agency_query.append(agency_query)

        return [
            ES_Q("bool", should=awarding_agency_query, minimum_should_match=1),
            ES_Q("bool", should=funding_agency_query, minimum_should_match=1),
        ]


class _RecipientSearchText(_Filter):
    underscore_name = "recipient_search_text"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: _QueryType) -> ES_Q:
        recipient_search_query = []
        fields = ["recipient_name"]

        for v in filter_values:
            upper_recipient_string = es_sanitize(v.upper())
            query = es_sanitize(upper_recipient_string) + "*"
            if "\\" in es_sanitize(upper_recipient_string):
                query = es_sanitize(upper_recipient_string) + r"\*"
            recipient_name_query = ES_Q("query_string", query=query, default_operator="AND", fields=fields)

            if len(upper_recipient_string) == 9 and upper_recipient_string[:5].isnumeric():
                recipient_duns_query = ES_Q("match", recipient_unique_id=upper_recipient_string)
                recipient_search_query.append(ES_Q("dis_max", queries=[recipient_name_query, recipient_duns_query]))
            else:
                recipient_search_query.append(recipient_name_query)

        return ES_Q("bool", should=recipient_search_query, minimum_should_match=1)


class _RecipientId(_Filter):
    underscore_name = "recipient_id"

    @classmethod
    def generate_elasticsearch_query(cls, filter_value: str, query_type: _QueryType) -> ES_Q:
        recipient_hash = filter_value[:-2]
        if filter_value.endswith("P"):
            return ES_Q("match", parent_recipient_hash=recipient_hash)
        elif filter_value.endswith("C"):
            return ES_Q("match", recipient_hash=recipient_hash) & ~ES_Q(
                "match", parent_recipient_unique_id__keyword="NULL"
            )
        else:
            return ES_Q("match", recipient_hash=recipient_hash) & ES_Q(
                "match", parent_recipient_unique_id__keyword="NULL"
            )


class _RecipientScope(_Filter):
    underscore_name = "recipient_scope"

    @classmethod
    def generate_elasticsearch_query(cls, filter_value: str, query_type: _QueryType) -> ES_Q:
        recipient_scope_query = ES_Q("match", recipient_location_country_code="USA")

        if filter_value == "domestic":
            return recipient_scope_query
        elif filter_value == "foreign":
            return ~recipient_scope_query


class _RecipientLocations(_Filter):
    underscore_name = "recipient_locations"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[dict], query_type: _QueryType) -> ES_Q:
        recipient_locations_query = []

        for v in filter_values:
            location_query = []
            location_lookup = {
                "country_code": v.get("country"),
                "state_code": v.get("state"),
                "county_code": v.get("county"),
                "congressional_code": v.get("district"),
                "city_name__keyword": v.get("city"),
                "zip5": v.get("zip"),
            }

            for location_key, location_value in location_lookup.items():
                if location_value is not None:
                    location_value = location_value.upper()
                    location_query.append(ES_Q("match", **{f"recipient_location_{location_key}": location_value}))

            recipient_locations_query.append(ES_Q("bool", must=location_query))

        return ES_Q("bool", should=recipient_locations_query, minimum_should_match=1)


class _RecipientTypeNames(_Filter):
    underscore_name = "recipient_type_names"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: _QueryType) -> ES_Q:
        recipient_type_query = []

        for v in filter_values:
            recipient_type_query.append(ES_Q("match", business_categories=v))

        return ES_Q("bool", should=recipient_type_query, minimum_should_match=1)


class _PlaceOfPerformanceScope(_Filter):
    underscore_name = "place_of_performance_scope"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: _QueryType) -> ES_Q:
        pop_scope_query = ES_Q("match", pop_country_code="USA")

        if filter_values == "domestic":
            return pop_scope_query
        elif filter_values == "foreign":
            return ~pop_scope_query


class _PlaceOfPerformanceLocations(_Filter):
    underscore_name = "place_of_performance_locations"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[dict], query_type: _QueryType) -> ES_Q:
        pop_locations_query = []

        for v in filter_values:
            location_query = []
            location_lookup = {
                "country_code": v.get("country"),
                "state_code": v.get("state"),
                "county_code": v.get("county"),
                "congressional_code": v.get("district"),
                "city_name__keyword": v.get("city"),
                "zip5": v.get("zip"),
            }

            for location_key, location_value in location_lookup.items():
                if location_value is not None:
                    location_value = location_value.upper()
                    location_query.append(ES_Q("match", **{f"pop_{location_key}": location_value}))

            pop_locations_query.append(ES_Q("bool", must=location_query))

        return ES_Q("bool", should=pop_locations_query, minimum_should_match=1)


class _AwardAmounts(_Filter):
    underscore_name = "award_amounts"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[dict], query_type: _QueryType) -> ES_Q:
        award_amounts_query = []
        for v in filter_values:
            lower_bound = v.get("lower_bound")
            upper_bound = v.get("upper_bound")
            award_amounts_query.append(ES_Q("range", award_amount={"gte": lower_bound, "lte": upper_bound}))
        return ES_Q("bool", should=award_amounts_query, minimum_should_match=1)


class _AwardIds(_Filter):
    underscore_name = "award_ids"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: _QueryType) -> ES_Q:
        award_ids_query = []

        for v in filter_values:
            if v and v.startswith('"') and v.endswith('"'):
                v = v[1:-1]
                award_ids_query.append(ES_Q("term", display_award_id={"value": es_sanitize(v)}))
            else:
                v = es_sanitize(v)
                v = " +".join(v.split())
                award_ids_query.append(ES_Q("regexp", display_award_id={"value": v}))

        return ES_Q("bool", should=award_ids_query, minimum_should_match=1)


class _ProgramNumbers(_Filter):
    underscore_name = "program_numbers"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: _QueryType) -> ES_Q:
        programs_numbers_query = []

        for v in filter_values:
            programs_numbers_query.append(ES_Q("match", cfda_number=v))

        return ES_Q("bool", should=programs_numbers_query, minimum_should_match=1)


class _ContractPricingTypeCodes(_Filter):
    underscore_name = "contract_pricing_type_codes"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: _QueryType) -> ES_Q:
        contract_pricing_query = []

        for v in filter_values:
            contract_pricing_query.append(ES_Q("match", type_of_contract_pricing__keyword=v))

        return ES_Q("bool", should=contract_pricing_query, minimum_should_match=1)


class _SetAsideTypeCodes(_Filter):
    underscore_name = "set_aside_type_codes"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: _QueryType) -> ES_Q:
        set_aside_query = []

        for v in filter_values:
            set_aside_query.append(ES_Q("match", type_set_aside__keyword=v))

        return ES_Q("bool", should=set_aside_query, minimum_should_match=1)


class _ExtentCompetedTypeCodes(_Filter):
    underscore_name = "extent_competed_type_codes"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: _QueryType) -> ES_Q:
        extent_competed_query = []

        for v in filter_values:
            extent_competed_query.append(ES_Q("match", extent_competed__keyword=v))

        return ES_Q("bool", should=extent_competed_query, minimum_should_match=1)


class _DisasterEmergencyFundCodes(_Filter):
    """Disaster and Emergency Fund Code filters"""

    underscore_name = "def_codes"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: _QueryType) -> ES_Q:
        def_codes_query = []
        amounts_query = [
            ES_Q("bool", must_not=ES_Q("match", total_covid_obligation=0)),
            ES_Q("bool", must_not=ES_Q("match", total_covid_outlay=0)),
        ]
        for v in filter_values:
            def_codes_query.append(ES_Q("match", disaster_emergency_fund_codes=v))
        if query_type == _QueryType.AWARDS:
            return [
                ES_Q("bool", should=def_codes_query, minimum_should_match=1),
                ES_Q("bool", should=amounts_query, minimum_should_match=1),
            ]
        return ES_Q(
            "bool",
            should=def_codes_query,
            minimum_should_match=1,
            must=ES_Q("range", action_date={"gte": "2020-04-01"}),
        )


class _QueryText(_Filter):
    """Query text with a specific field to search on (i.e. {'text': <query_text>, 'fields': [<field_to_search_on>]})"""

    underscore_name = "query"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: dict, query_type: _QueryType) -> ES_Q:
        query_text = es_sanitize(filter_values["text"])
        query_fields = filter_values["fields"]
        return ES_Q("multi_match", query=query_text, type="phrase_prefix", fields=query_fields)


class _NonzeroFields(_Filter):
    """List of fields where at least one should have a nonzero value for each document"""

    underscore_name = "nonzero_fields"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: _QueryType) -> ES_Q:
        non_zero_queries = []
        for field in filter_values:
            non_zero_queries.append(ES_Q("range", **{field: {"gt": 0}}))
            non_zero_queries.append(ES_Q("range", **{field: {"lt": 0}}))
        return ES_Q("bool", should=non_zero_queries, minimum_should_match=1)


class QueryWithFilters:

    filter_lookup = {
        _Keywords.underscore_name: _Keywords,
        _KeywordSearch.underscore_name: _KeywordSearch,
        _TimePeriods.underscore_name: _TimePeriods,
        _AwardTypeCodes.underscore_name: _AwardTypeCodes,
        _Agencies.underscore_name: _Agencies,
        _RecipientSearchText.underscore_name: _RecipientSearchText,
        _RecipientId.underscore_name: _RecipientId,
        _RecipientScope.underscore_name: _RecipientScope,
        _RecipientLocations.underscore_name: _RecipientLocations,
        _RecipientTypeNames.underscore_name: _RecipientTypeNames,
        _PlaceOfPerformanceScope.underscore_name: _PlaceOfPerformanceScope,
        _PlaceOfPerformanceLocations.underscore_name: _PlaceOfPerformanceLocations,
        _AwardAmounts.underscore_name: _AwardAmounts,
        _AwardIds.underscore_name: _AwardIds,
        _ProgramNumbers.underscore_name: _ProgramNumbers,
        NaicsCodes.underscore_name: NaicsCodes,
        PSCCodes.underscore_name: PSCCodes,
        _ContractPricingTypeCodes.underscore_name: _ContractPricingTypeCodes,
        _SetAsideTypeCodes.underscore_name: _SetAsideTypeCodes,
        _ExtentCompetedTypeCodes.underscore_name: _ExtentCompetedTypeCodes,
        _DisasterEmergencyFundCodes.underscore_name: _DisasterEmergencyFundCodes,
        _QueryText.underscore_name: _QueryText,
        _NonzeroFields.underscore_name: _NonzeroFields,
    }

    unsupported_filters = ["legal_entities"]

    @classmethod
    def _generate_elasticsearch_query(cls, filters: dict, query_type: _QueryType) -> ES_Q:
        must_queries = []

        # tas_codes are unique in that the same query is spread across two keys
        must_queries = cls._handle_tas_query(must_queries, filters, query_type)

        for filter_type, filter_values in filters.items():
            # Validate the filters
            if filter_type in cls.unsupported_filters:
                msg = "API request included '{}' key. No filtering will occur with provided value '{}'"
                logger.warning(msg.format(filter_type, filter_values))
                continue
            elif filter_type not in cls.filter_lookup.keys():
                raise InvalidParameterException(f"Invalid filter: {filter_type} does not exist.")

            # Generate the query for a filter
            query = cls.filter_lookup[filter_type].generate_query(filter_values, query_type)

            # Handle the possibility of multiple queries from one filter
            if isinstance(query, list):
                must_queries.extend(query)
            else:
                must_queries.append(query)
        return ES_Q("bool", must=must_queries)

    @classmethod
    def _handle_tas_query(cls, must_queries: list, filters: dict, query_type: _QueryType) -> list:
        if filters.get(TreasuryAccounts.underscore_name) or filters.get(TasCodes.underscore_name):
            tas_queries = []
            if filters.get(TreasuryAccounts.underscore_name):
                tas_queries.append(
                    TreasuryAccounts.generate_elasticsearch_query(filters[TreasuryAccounts.underscore_name], query_type)
                )
            if filters.get(TasCodes.underscore_name):
                tas_queries.append(
                    (TasCodes.generate_elasticsearch_query(filters[TasCodes.underscore_name], query_type))
                )
            must_queries.append(ES_Q("bool", should=tas_queries, minimum_should_match=1))
            filters.pop(TreasuryAccounts.underscore_name, None)
            filters.pop(TasCodes.underscore_name, None)
        return must_queries

    @classmethod
    def generate_awards_elasticsearch_query(cls, filters: dict) -> ES_Q:
        return cls._generate_elasticsearch_query(filters, _QueryType.AWARDS)

    @classmethod
    def generate_transactions_elasticsearch_query(cls, filters: dict) -> ES_Q:
        return cls._generate_elasticsearch_query(filters, _QueryType.TRANSACTIONS)
