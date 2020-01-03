import logging

from django.conf import settings

from abc import abstractmethod, ABCMeta
from typing import Union, List
from elasticsearch_dsl import Q as ES_Q

from usaspending_api.common.exceptions import InvalidParameterException


logger = logging.getLogger(__name__)


class _Filter(metaclass=ABCMeta):
    """
    Represents a filter object used to query currently only Elasticsearch. The idea is that this
    ABC and QueryWithFilters can be altered to work with Django as well down the road.
    """

    underscore_name = None

    @classmethod
    def generate_query(cls, filter_values: Union[str, list]) -> dict:

        if filter_values is None:
            raise InvalidParameterException(f"Invalid filter: {cls.underscore_name} has null as its value.")

        return cls._generate_elasticsearch_query(filter_values)

    @classmethod
    @abstractmethod
    def _generate_elasticsearch_query(cls, filter_values: Union[str, list]) -> Union[ES_Q, List[ES_Q]]:
        """ Returns a Q object used to query Elasticsearch. """
        raise NotImplementedError("Must be implemented in subclasses of _Filter.")


class _Keywords(_Filter):
    underscore_name = "keywords"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_values: List[str]) -> ES_Q:
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
        ]
        for v in filter_values:
            keyword_queries.append(ES_Q("query_string", query=v, default_operator="AND", fields=fields))

        return ES_Q("dis_max", queries=keyword_queries)


class _TimePeriods(_Filter):
    underscore_name = "time_period"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_values: List[dict]) -> ES_Q:
        time_period_query = []

        for v in filter_values:
            start_date = v.get("start_date") or settings.API_SEARCH_MIN_DATE
            end_date = v.get("end_date") or settings.API_MAX_DATE
            time_period_query.append(ES_Q("range", action_date={"gte": start_date, "lte": end_date}))

        return ES_Q("bool", should=time_period_query, minimum_should_match=1)


class _AwardTypeCodes(_Filter):
    underscore_name = "award_type_codes"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_values: List[str]) -> ES_Q:
        award_type_codes_query = []

        for v in filter_values:
            award_type_codes_query.append(ES_Q("match", type=v))

        return ES_Q("bool", should=award_type_codes_query, minimum_should_match=1)


class _Agencies(_Filter):
    underscore_name = "agencies"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_values: List[dict]) -> List[ES_Q]:
        awarding_agency_query = []
        funding_agency_query = []

        for v in filter_values:
            agency_name = v["name"]
            agency_tier = v["tier"]
            agency_type = v["type"]
            agency_query = ES_Q("match", **{f"{agency_type}_{agency_tier}_agency_name__keyword": agency_name})
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
    def _generate_elasticsearch_query(cls, filter_values: List[str]) -> ES_Q:
        recipient_search_query = []
        fields = ["recipient_name"]

        for v in filter_values:
            upper_recipient_string = v.upper()
            recipient_name_query = ES_Q(
                "query_string", query=upper_recipient_string, default_operator="AND", fields=fields
            )

            if len(upper_recipient_string) == 9 and upper_recipient_string[:5].isnumeric():
                recipient_duns_query = ES_Q("match", recipient_unique_id=upper_recipient_string)
                recipient_search_query.append(ES_Q("dis_max", queries=[recipient_name_query, recipient_duns_query]))
            else:
                recipient_search_query.append(recipient_name_query)

        return ES_Q("bool", should=recipient_search_query, minimum_should_match=1)


class _RecipientId(_Filter):
    underscore_name = "recipient_id"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_value: str) -> ES_Q:
        recipient_hash = filter_value[:-2]
        if filter_value.endswith("P"):
            return ES_Q("match", parent_recipient_hash=recipient_hash)
        elif filter_value.endswith("C"):
            return ES_Q("match", recipient_hash=recipient_hash) & ~ES_Q("match", parent_recipient_unique_id="NULL")
        else:
            return ES_Q("match", recipient_hash=recipient_hash) & ES_Q("match", parent_recipient_unique_id="NULL")


class _RecipientScope(_Filter):
    underscore_name = "recipient_scope"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_value: str) -> ES_Q:
        recipient_scope_query = ES_Q("match", recipient_location_country_code="USA")

        if filter_value == "domestic":
            return recipient_scope_query
        elif filter_value == "foreign":
            return ~recipient_scope_query


class _RecipientLocations(_Filter):
    underscore_name = "recipient_locations"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_values: List[dict]) -> ES_Q:
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
                    location_query.append(ES_Q("match", **{f"recipient_location_{location_key}": location_value}))

            recipient_locations_query.append(ES_Q("bool", must=location_query))

        return ES_Q("bool", should=recipient_locations_query, minimum_should_match=1)


class _RecipientTypeNames(_Filter):
    underscore_name = "recipient_type_names"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_values: List[str]) -> ES_Q:
        recipient_type_query = []

        for v in filter_values:
            recipient_type_query.append(ES_Q("match", business_categories=v))

        return ES_Q("bool", should=recipient_type_query, minimum_should_match=1)


class _PlaceOfPerformanceScope(_Filter):
    underscore_name = "place_of_performance_scope"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_values: List[str]) -> ES_Q:
        pop_scope_query = ES_Q("match", pop_country_code="USA")

        if filter_values == "domestic":
            return pop_scope_query
        elif filter_values == "foreign":
            return ~pop_scope_query


class _PlaceOfPerformanceLocations(_Filter):
    underscore_name = "place_of_performance_locations"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_values: List[dict]) -> ES_Q:
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
                    location_query.append(ES_Q("match", **{f"pop_{location_key}": location_value}))

            pop_locations_query.append(ES_Q("bool", must=location_query))

        return ES_Q("bool", should=pop_locations_query, minimum_should_match=1)


class _AwardAmounts(_Filter):
    underscore_name = "award_amounts"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_values: List[dict]) -> ES_Q:
        award_amounts_query = []

        for v in filter_values:
            lower_bound = v.get("lower_bound")
            upper_bound = v.get("upper_bound")
            award_amounts_query.append(ES_Q("range", award_amount={"gte": lower_bound, "lte": upper_bound}))

        return ES_Q("bool", should=award_amounts_query, minimum_should_match=1)


class _AwardIds(_Filter):
    underscore_name = "award_ids"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_values: List[dict]) -> ES_Q:
        award_ids_query = []

        for v in filter_values:
            award_ids_query.append(ES_Q("match", display_award_id=v))

        return ES_Q("bool", should=award_ids_query, minimum_should_match=1)


class _ProgramNumbers(_Filter):
    underscore_name = "program_numbers"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_values: List[str]) -> ES_Q:
        programs_numbers_query = []

        for v in filter_values:
            programs_numbers_query.append(ES_Q("match", cfda_number=v))

        return ES_Q("bool", should=programs_numbers_query, minimum_should_match=1)


class _NaicsCodes(_Filter):
    underscore_name = "naics_codes"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_values: List[str]) -> ES_Q:
        naics_codes_query = []

        for v in filter_values:
            naics_codes_query.append(ES_Q("match", naics_code__keyword=v))

        return ES_Q("bool", should=naics_codes_query, minimum_should_match=1)


class _PscCodes(_Filter):
    underscore_name = "psc_codes"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_values: List[str]) -> ES_Q:
        psc_codes_query = []

        for v in filter_values:
            psc_codes_query.append(ES_Q("match", product_or_service_code__keyword=v))

        return ES_Q("bool", should=psc_codes_query, minimum_should_match=1)


class _ContractPricingTypeCodes(_Filter):
    underscore_name = "contract_pricing_type_codes"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_values: List[str]) -> ES_Q:
        contract_pricing_query = []

        for v in filter_values:
            contract_pricing_query.append(ES_Q("match", type_of_contract_pricing__keyword=v))

        return ES_Q("bool", should=contract_pricing_query, minimum_should_match=1)


class _SetAsideTypeCodes(_Filter):
    underscore_name = "set_aside_type_codes"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_values: List[str]) -> ES_Q:
        set_aside_query = []

        for v in filter_values:
            set_aside_query.append(ES_Q("match", type_set_aside__keyword=v))

        return ES_Q("bool", should=set_aside_query, minimum_should_match=1)


class _ExtentCompetedTypeCodes(_Filter):
    underscore_name = "extent_competed_type_codes"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_values: List[str]) -> ES_Q:
        extent_competed_query = []

        for v in filter_values:
            extent_competed_query.append(ES_Q("match", extent_competed__keyword=v))

        return ES_Q("bool", should=extent_competed_query, minimum_should_match=1)


class _TasCodes(_Filter):
    underscore_name = "tas_codes"

    @classmethod
    def _generate_elasticsearch_query(cls, filter_values: List[dict]) -> ES_Q:
        tas_codes_query = []

        for v in filter_values:
            code_query = []
            code_lookup = {
                "aid": v.get("aid"),
                "ata": v.get("ata"),
                "main": v.get("main"),
                "sub": v.get("sub"),
                "bpoa": v.get("bpoa"),
                "epoa": v.get("epoa"),
                "a": v.get("a"),
            }

            for code_key, code_value in code_lookup.items():
                if code_value is not None:
                    code_query.append(ES_Q("match", **{f"treasury_accounts__{code_key}": code_value}))

            tas_codes_query.append(ES_Q("bool", must=code_query))

        nested_query = ES_Q("bool", should=tas_codes_query, minimum_should_match=1)
        return ES_Q("nested", path="treasury_accounts", query=nested_query)


class QueryWithFilters:

    filter_lookup = {
        _Keywords.underscore_name: _Keywords,
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
        _NaicsCodes.underscore_name: _NaicsCodes,
        _PscCodes.underscore_name: _PscCodes,
        _ContractPricingTypeCodes.underscore_name: _ContractPricingTypeCodes,
        _SetAsideTypeCodes.underscore_name: _SetAsideTypeCodes,
        _ExtentCompetedTypeCodes.underscore_name: _ExtentCompetedTypeCodes,
        _TasCodes.underscore_name: _TasCodes,
    }

    unsupported_filters = ["legal_entities"]

    @classmethod
    def generate_elasticsearch_query(cls, filters: dict) -> ES_Q:
        must_queries = []

        for filter_type, filter_values in filters.items():
            # Validate the filters
            if filter_type in cls.unsupported_filters:
                msg = "API request included '{}' key. No filtering will occur with provided value '{}'"
                logger.info(msg.format(filter_type, filter_values))
                continue
            elif filter_type not in cls.filter_lookup.keys():
                raise InvalidParameterException(f"Invalid filter: {filter_type} does not exist.")

            # Generate the query for a filter
            query = cls.filter_lookup[filter_type].generate_query(filter_values)

            # Handle the possibility of multiple queries from one filter
            if isinstance(query, list):
                must_queries.extend(query)
            else:
                must_queries.append(query)

        return ES_Q("bool", must=must_queries)
