from abc import ABC, abstractmethod
import re
from typing import List, Tuple
from django.conf import settings
from datetime import datetime
from elasticsearch_dsl import Q as ES_Q
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.search.v2.es_sanitization import es_sanitize
from usaspending_api.references.models import DisasterEmergencyFundCode

class MiscStrategy(ABC):

    def get_query(self, filter_type: str, filter_values, query_type: QueryType) -> ES_Q:
        match filter_type:
            case "time_period":
                return _TimePeriods.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type)
            case "description":
                return _Description.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type)
            case "prime_and_sub_award_types":
                return _SubawardsPrimeSubAwardTypes.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type)
            case "agencies":
                return _Agencies._build_query_object(filter_values=filter_values, query_type=query_type)
            case "def_codes":
                return _DisasterEmergencyFundCodes.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type)
            case "query":
                return _QueryText.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type)
            case "nonzero_fields":
                return _NonzeroFields.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type)
            case "award_unique_id":
                return _AwardUniqueId.generate_elasticsearch_query(filter_values=filter_values, query_type=query_type)
            case _:
                raise InvalidParameterException(f"Invalid filter: {filter_type} does not exist.")

class _Description(ABC):
    @classmethod
    def generate_elasticsearch_query(cls, filter_values: str, query_type: QueryType, **options) -> ES_Q:
        fields = {
            QueryType.AWARDS: ["description"],
            QueryType.SUBAWARDS: ["subaward_description"],
            QueryType.TRANSACTIONS: ["transaction_description"],
        }
        query = es_sanitize(filter_values)

        return ES_Q("multi_match", query=query, fields=fields.get(query_type, []), type="phrase_prefix")

class _TimePeriods(ABC):
    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[dict], query_type: QueryType, **options) -> ES_Q:

        # Temporary until rest of dependencies are updated
        if "time_period_obj" not in options or options.get("time_period_obj") is None:
            return cls._default_elasticsearch_query(filter_values, query_type, **options)

        time_period_query = []
        for filter_value in filter_values:
            time_period_obj = options["time_period_obj"]
            time_period_obj.filter_value = filter_value

            gte_range = time_period_obj.gte_date_range()
            lte_range = time_period_obj.lte_date_range()

            all_ranges = []
            for range in gte_range:
                all_ranges.append(ES_Q("bool", should=[ES_Q("range", **range)]))

            for range in lte_range:
                all_ranges.append(ES_Q("bool", should=[ES_Q("range", **range)]))

            time_period_query.append(ES_Q("bool", should=all_ranges, minimum_should_match="100%"))

        return ES_Q("bool", should=time_period_query, minimum_should_match=1)

    @classmethod
    def _default_elasticsearch_query(cls, filter_values: List[dict], query_type: QueryType, **options):
        time_period_query = []
        for filter_value in filter_values:
            start_date = filter_value.get("start_date") or settings.API_SEARCH_MIN_DATE
            end_date = filter_value.get("end_date") or settings.API_MAX_DATE

            gte_range = {filter_value.get("gte_date_type", "action_date"): {"gte": start_date}}
            lte_range = {
                filter_value.get("lte_date_type", "date_signed" if query_type == QueryType.AWARDS else "action_date"): {
                    "lte": end_date
                }
            }

            time_period_query.append(
                ES_Q("bool", should=[ES_Q("range", **gte_range), ES_Q("range", **lte_range)], minimum_should_match=2)
            )

        return ES_Q("bool", should=time_period_query, minimum_should_match=1)

class _SubawardsPrimeSubAwardTypes(ABC):
    """Intended for subawards' Querytype that makes this query compatible with Subawards."""

    underscore_name = "prime_and_sub_award_types"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
        award_type_codes_query = []

        award_types = filter_values.get("elasticsearch_sub_awards")
        for type in award_types:
            award_type_codes_query.append(ES_Q("match", **{"prime_award_group": type}))
        return ES_Q("bool", should=award_type_codes_query, minimum_should_match=1)

class _Agencies(ABC):
    underscore_name = "agencies"

    @staticmethod
    def _build_query_object(filter_values: dict, query_type: QueryType) -> Tuple[str, ES_Q]:
        agency_name = filter_values.get("name")
        agency_toptier_code = filter_values.get("toptier_code")
        agency_tier = filter_values["tier"]
        agency_type = filter_values["type"]
        toptier_id = filter_values.get("toptier_id")
        toptier_name = filter_values.get("toptier_name")

        query_object = ES_Q()

        if query_type == QueryType.AWARDS:
            if agency_toptier_code:
                query_object &= ES_Q(
                    "match", **{f"{agency_type}_{agency_tier}_agency_code__keyword": agency_toptier_code}
                )
        elif query_type == QueryType.TRANSACTIONS:
            if toptier_id:
                if toptier_name and toptier_name != "awarding":
                    raise InvalidParameterException(
                        "Incompatible parameters: `toptier_id` can only be used with `awarding` agency type."
                    )
                query_object &= ES_Q("match", **{"awarding_toptier_agency_id": toptier_id})

        if agency_name:
            query_object &= ES_Q("match", **{f"{agency_type}_{agency_tier}_agency_name__keyword": agency_name})

        if agency_tier == "subtier" and toptier_name is not None:
            query_object &= ES_Q("match", **{f"{agency_type}_toptier_agency_name__keyword": toptier_name})

        return agency_type, query_object

class _DisasterEmergencyFundCodes(ABC):
    """Disaster and Emergency Fund Code filters"""

    underscore_name = "def_codes"

    @classmethod
    def _generate_covid_iija_es_queries_transactions(cls, def_code_field, covid_filters, iija_filters):
        covid_es_queries = [
            ES_Q("match", **{def_code_field: def_code})
            & ES_Q("range", action_date={"gte": datetime.strftime(enactment_date, "%Y-%m-%d")})
            for def_code, enactment_date in covid_filters.items()
        ]
        iija_es_queries = [
            ES_Q("match", **{def_code_field: def_code})
            & ES_Q("range", action_date={"gte": datetime.strftime(enactment_date, "%Y-%m-%d")})
            for def_code, enactment_date in iija_filters.items()
        ]

        return covid_es_queries, iija_es_queries

    @classmethod
    def _generate_covid_iija_es_queries_subawards(cls, def_code_field, covid_filters, iija_filters):
        covid_es_queries = [
            ES_Q("match", **{def_code_field: def_code})
            & ES_Q("range", **{"sub_action_date": {"gte": datetime.strftime(enactment_date, "%Y-%m-%d")}})
            for def_code, enactment_date in covid_filters.items()
        ]
        iija_es_queries = [
            ES_Q("match", **{def_code_field: def_code})
            & ES_Q("range", **{"sub_action_date": {"gte": datetime.strftime(enactment_date, "%Y-%m-%d")}})
            for def_code, enactment_date in iija_filters.items()
        ]

        return covid_es_queries, iija_es_queries

    @classmethod
    def _generate_covid_iija_es_queries_other(cls, def_code_field, covid_filters, iija_filters):
        covid_es_queries = [ES_Q("match", **{def_code_field: def_code}) for def_code in covid_filters.keys()]
        iija_es_queries = [ES_Q("match", **{def_code_field: def_code}) for def_code in iija_filters.keys()]

        return covid_es_queries, iija_es_queries

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
        nested_path = options.get("nested_path", "")
        def_codes_query = []
        def_code_field = f"{nested_path}{'.' if nested_path else ''}disaster_emergency_fund_code{'s' if query_type != QueryType.ACCOUNTS else ''}"

        # Get all COVID and IIJA disaster codes from the database
        covid_disaster_codes = list(
            DisasterEmergencyFundCode.objects.filter(group_name="covid_19").values_list("code", flat=True)
        )
        iija_disaster_codes = list(
            DisasterEmergencyFundCode.objects.filter(group_name="infrastructure").values_list("code", flat=True)
        )

        all_covid_iija_defc = set(
            DisasterEmergencyFundCode.objects.filter(group_name__in=["covid_19", "infrastructure"]).values_list(
                "code", "earliest_public_law_enactment_date"
            )
        )
        all_covid_iija_defc = dict(all_covid_iija_defc)

        # Dictionaries with the given COVID or IIJA DEF code(s) and their associated enactment date
        covid_filters = {
            code: enactment_date
            for code, enactment_date in all_covid_iija_defc.items()
            if code in covid_disaster_codes and code in filter_values
        }
        iija_filters = {
            code: enactment_date
            for code, enactment_date in all_covid_iija_defc.items()
            if code in iija_disaster_codes and code in filter_values
        }

        # Everything that isn't COVID or IIJA
        other_filters = list(set(filter_values) - set(all_covid_iija_defc.keys()))
        other_queries = [ES_Q("match", **{def_code_field: filter_value}) for filter_value in other_filters]

        # Filter on the `disaster_emergency_fund_code` AND `action_date` values for transactions
        if query_type == QueryType.TRANSACTIONS:
            covid_es_queries, iija_es_queries = cls._generate_covid_iija_es_queries_transactions(
                def_code_field, covid_filters, iija_filters
            )

            if covid_es_queries or iija_es_queries:
                covid_iija_queries = covid_es_queries + iija_es_queries
                def_codes_query.append(
                    ES_Q(
                        "bool",
                        should=covid_iija_queries,
                        minimum_should_match=1,
                    )
                )

        # Filter on the `disaster_emergency_fund_code` AND `sub_action_date` values for subawards
        elif query_type == QueryType.SUBAWARDS:
            covid_es_queries, iija_es_queries = cls._generate_covid_iija_es_queries_subawards(
                def_code_field, covid_filters, iija_filters
            )

            if covid_es_queries or iija_es_queries:
                covid_iija_queries = covid_es_queries + iija_es_queries
                def_codes_query.append(
                    ES_Q(
                        "bool",
                        should=covid_iija_queries,
                        minimum_should_match=1,
                    )
                )

        # Only filter on the DEFC value, but also filter out results where
        #   `covid/iija_outlay` and `covid/iija_obligation` are 0
        elif query_type == QueryType.AWARDS:
            covid_es_queries, iija_es_queries = cls._generate_covid_iija_es_queries_other(
                def_code_field, covid_filters, iija_filters
            )

            if covid_es_queries:
                covid_nonzero_limit = _NonzeroFields.generate_elasticsearch_query(
                    ["total_covid_outlay", "total_covid_obligation"], query_type
                )
                covid_nonzero_query = ES_Q(
                    "bool", must=[covid_nonzero_limit], should=covid_es_queries, minimum_should_match=1
                )

                def_codes_query.append(ES_Q("bool", should=covid_nonzero_query, minimum_should_match=1))

            if iija_es_queries:
                iija_nonzero_limit = _NonzeroFields.generate_elasticsearch_query(
                    ["total_iija_outlay", "total_iija_obligation"], query_type
                )
                iija_nonzero_query = ES_Q(
                    "bool", must=[iija_nonzero_limit], should=iija_es_queries, minimum_should_match=1
                )

                def_codes_query.append(ES_Q("bool", should=iija_nonzero_query, minimum_should_match=1))

        # Only filter on the DEFC value for other types of queries
        else:
            covid_es_queries, iija_es_queries = cls._generate_covid_iija_es_queries_other(
                def_code_field, covid_filters, iija_filters
            )

            if covid_es_queries or iija_es_queries:
                covid_iija_queries = covid_es_queries + iija_es_queries

                def_codes_query.append(ES_Q("bool", should=covid_iija_queries, minimum_should_match=1))

        if other_queries:
            def_codes_query.append(ES_Q("bool", should=other_queries, minimum_should_match=1))

        if len(def_codes_query) != 1:
            final_query = ES_Q("bool", should=def_codes_query, minimum_should_match=1)
        else:
            final_query = def_codes_query[0]

        return final_query


class _QueryText(ABC):
    """Query text with a specific field to search on (i.e. {'text': <query_text>, 'fields': [<field_to_search_on>]})"""

    underscore_name = "query"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: dict, query_type: QueryType, **options) -> ES_Q:
        nested_path = options.get("nested_path", "")
        query_text = filter_values["text"]
        query_fields = [f"{nested_path}{'.' if nested_path else ''}{field}" for field in filter_values["fields"]]
        return ES_Q("multi_match", query=query_text, type="phrase_prefix", fields=query_fields)


class _NonzeroFields(ABC):
    """List of fields where at least one should have a nonzero value for each document"""

    underscore_name = "nonzero_fields"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
        nested_path = options.get("nested_path", "")
        non_zero_queries = []
        for field in filter_values:
            field_name = f"{nested_path}{'.' if nested_path else ''}{field}"
            non_zero_queries.append(ES_Q("range", **{field_name: {"gt": 0}}))
            non_zero_queries.append(ES_Q("range", **{field_name: {"lt": 0}}))
        return ES_Q("bool", should=non_zero_queries, minimum_should_match=1)


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