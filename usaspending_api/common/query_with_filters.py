import copy
import itertools
import logging
import re
from datetime import datetime
from typing import List, Tuple

from django.conf import settings
from elasticsearch_dsl import Q as ES_Q

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.api_helper import (
    DUPLICATE_DISTRICT_LOCATION_PARAMETERS,
    INCOMPATIBLE_DISTRICT_LOCATION_PARAMETERS,
)
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.references.models.psc import PSC
from usaspending_api.search.filters.elasticsearch.filter import QueryType, _Filter
from usaspending_api.search.filters.elasticsearch.naics import NaicsCodes
from usaspending_api.search.filters.elasticsearch.psc import PSCCodes
from usaspending_api.search.filters.elasticsearch.tas import TasCodes, TreasuryAccounts
from usaspending_api.search.filters.time_period.decorators import NewAwardsOnlyTimePeriod
from usaspending_api.search.filters.time_period.query_types import (
    AwardSearchTimePeriod,
    SubawardSearchTimePeriod,
    TransactionSearchTimePeriod,
)
from usaspending_api.search.v2.es_sanitization import es_sanitize

logger = logging.getLogger(__name__)


class _SubawardsKeywords(_Filter):
    """Intended for subawards' Querytype that makes keyword queries compatible with Subawards."""

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
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


class _Keywords(_Filter):
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


class _Description(_Filter):
    underscore_name = "description"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: str, query_type: QueryType, **options) -> ES_Q:
        fields = {
            QueryType.AWARDS: ["description"],
            QueryType.SUBAWARDS: ["subaward_description"],
            QueryType.TRANSACTIONS: ["transaction_description"],
        }
        query = es_sanitize(filter_values)

        return ES_Q("multi_match", query=query, fields=fields.get(query_type, []), type="phrase_prefix")


class _KeywordSearch(_Filter):
    underscore_name = "keyword_search"

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


class _TransactionKeywordSearch(_Filter):
    """Intended for subawards' and awards' Querytype that makes this query compatible with Subawards and Awards."""

    underscore_name = "transaction_keyword_search"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
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


class _TimePeriods(_Filter):
    underscore_name = "time_period"

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


class _AwardTypeCodes(_Filter):
    underscore_name = "award_type_codes"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
        award_type_codes_query = []

        for filter_value in filter_values:
            if query_type == QueryType.SUBAWARDS:
                type_ = "prime_award_type"
            else:
                type_ = "type"
            award_type_codes_query.append(ES_Q("match", **{type_: filter_value}))
        return ES_Q("bool", should=award_type_codes_query, minimum_should_match=1)


class _SubawardsPrimeSubAwardTypes(_Filter):
    """Intended for subawards' Querytype that makes this query compatible with Subawards."""

    underscore_name = "prime_and_sub_award_types"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
        award_type_codes_query = []

        award_types = filter_values.get("elasticsearch_sub_awards")
        for type in award_types:
            award_type_codes_query.append(ES_Q("match", **{"prime_award_group": type}))
        return ES_Q("bool", should=award_type_codes_query, minimum_should_match=1)


class _Agencies(_Filter):
    underscore_name = "agencies"

    @staticmethod
    def _build_query_object(filter_value: dict, query_type: QueryType) -> Tuple[str, ES_Q]:
        agency_name = filter_value.get("name")
        agency_toptier_code = filter_value.get("toptier_code")
        agency_tier = filter_value["tier"]
        agency_type = filter_value["type"]
        toptier_id = filter_value.get("toptier_id")
        toptier_name = filter_value.get("toptier_name")

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

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[dict], query_type: QueryType, **options) -> List[ES_Q]:
        awarding_agency_query = []
        funding_agency_query = []

        for filter_value in filter_values:
            agency_type, agency_query = cls._build_query_object(filter_value, query_type)

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
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
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


class _RecipientId(_Filter):
    underscore_name = "recipient_id"

    @classmethod
    def generate_elasticsearch_query(cls, filter_value: str, query_type: QueryType, **options) -> ES_Q:
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


class _RecipientScope(_Filter):
    underscore_name = "recipient_scope"

    @classmethod
    def generate_elasticsearch_query(cls, filter_value: str, query_type: QueryType, **options) -> ES_Q:
        if query_type == QueryType.SUBAWARDS:
            recipient_scope_query = ES_Q("match", sub_recipient_location_country_code="USA") | ES_Q(
                "match", sub_recipient_location_country_name="UNITED STATES"
            )
        else:
            recipient_scope_query = ES_Q("match", recipient_location_country_code="USA")

        if filter_value == "domestic":
            return recipient_scope_query
        elif filter_value == "foreign":
            return ~recipient_scope_query


class _RecipientLocations(_Filter):
    underscore_name = "recipient_locations"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[dict], query_type: QueryType, **options) -> ES_Q:
        recipient_locations_query = []

        for filter_value in filter_values:
            location_query = []
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

            for location_key, location_value in location_lookup.items():
                if (state is None or country != "USA" or county is not None) and (
                    district_current is not None or district_original is not None
                ):
                    raise InvalidParameterException(INCOMPATIBLE_DISTRICT_LOCATION_PARAMETERS)
                if location_value is not None:
                    location_value = location_value.upper()
                    if query_type == QueryType.SUBAWARDS:
                        location_query.append(
                            ES_Q("match", **{f"sub_recipient_location_{location_key}": location_value})
                        )
                    else:
                        location_query.append(ES_Q("match", **{f"recipient_location_{location_key}": location_value}))

            recipient_locations_query.append(ES_Q("bool", must=location_query))

        return ES_Q("bool", should=recipient_locations_query, minimum_should_match=1)


class _RecipientTypeNames(_Filter):
    underscore_name = "recipient_type_names"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
        recipient_type_query = []

        for filter_value in filter_values:
            recipient_type_query.append(ES_Q("match", business_categories=filter_value))

        return ES_Q("bool", should=recipient_type_query, minimum_should_match=1)


class _PlaceOfPerformanceScope(_Filter):
    underscore_name = "place_of_performance_scope"

    @classmethod
    def generate_elasticsearch_query(cls, filter_value: str, query_type: QueryType, **options) -> ES_Q:
        if query_type == QueryType.SUBAWARDS:
            pop_scope_query = ES_Q("match", sub_pop_country_code="USA") | ES_Q(
                "match", sub_pop_country_name="UNITED STATES"
            )
        else:
            # If an ES record has a pop_country_code of "USA" OR if it has any value for the pop_state_code field
            #   then it's considered domestic. Since we only support domestic states/territories then any
            #   pop_state_code value means the pop_country_code would be "USA".
            pop_scope_query = ES_Q("match", pop_country_code="USA") | ES_Q("exists", field="pop_state_code")

        if filter_value == "domestic":
            return pop_scope_query
        elif filter_value == "foreign":
            return ~pop_scope_query


class _PlaceOfPerformanceLocations(_Filter):
    underscore_name = "place_of_performance_locations"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[dict], query_type: QueryType, **options) -> ES_Q:
        pop_locations_query = []
        for filter_value in filter_values:
            location_query = []
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
            }

            if query_type == QueryType.SUBAWARDS:
                location_lookup["zip"] = filter_value.get("zip")
            else:
                location_lookup["zip5"] = filter_value.get("zip")

            for location_key, location_value in location_lookup.items():
                _PlaceOfPerformanceLocations._validate_district(
                    state, country, county, district_current, district_original
                )

                if location_value is not None:
                    location_value = location_value.upper()
                    if query_type == QueryType.SUBAWARDS:
                        location_query.append(ES_Q("match", **{f"sub_pop_{location_key}": location_value}))
                    else:
                        location_query.append(ES_Q("match", **{f"pop_{location_key}": location_value}))
            pop_locations_query.append(ES_Q("bool", must=location_query))

        return ES_Q("bool", should=pop_locations_query, minimum_should_match=1)

    @staticmethod
    def _validate_district(state, country, county, district_current, district_original):
        if (state is None or country != "USA" or county is not None) and (
            district_current is not None or district_original is not None
        ):
            raise InvalidParameterException(INCOMPATIBLE_DISTRICT_LOCATION_PARAMETERS)
        if district_current is not None and district_original is not None:
            raise InvalidParameterException(DUPLICATE_DISTRICT_LOCATION_PARAMETERS)


class _AwardAmounts(_Filter):
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


class _AwardIds(_Filter):
    underscore_name = "award_ids"

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


class _ProgramNumbers(_Filter):
    underscore_name = "program_numbers"

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


class _ProgramActivities(_Filter):
    underscore_name = "program_activities"

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

            if temp_must:
                program_activity_match_queries.append(
                    ES_Q("nested", path="program_activities", query=ES_Q("bool", must=temp_must))
                )

        if len(program_activity_match_queries) == 0:
            return ~ES_Q()
        return ES_Q("bool", should=program_activity_match_queries, minimum_should_match=1)


class _ContractPricingTypeCodes(_Filter):
    underscore_name = "contract_pricing_type_codes"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
        contract_pricing_query = []

        for filter_value in filter_values:
            contract_pricing_query.append(ES_Q("match", type_of_contract_pricing__keyword=filter_value))

        return ES_Q("bool", should=contract_pricing_query, minimum_should_match=1)


class _SetAsideTypeCodes(_Filter):
    underscore_name = "set_aside_type_codes"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
        set_aside_query = []

        for filter_value in filter_values:
            set_aside_query.append(ES_Q("match", type_set_aside__keyword=filter_value))

        return ES_Q("bool", should=set_aside_query, minimum_should_match=1)


class _ExtentCompetedTypeCodes(_Filter):
    underscore_name = "extent_competed_type_codes"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: List[str], query_type: QueryType, **options) -> ES_Q:
        extent_competed_query = []

        for filter_value in filter_values:
            extent_competed_query.append(ES_Q("match", extent_competed__keyword=filter_value))

        return ES_Q("bool", should=extent_competed_query, minimum_should_match=1)


class _DisasterEmergencyFundCodes(_Filter):
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


class _QueryText(_Filter):
    """Query text with a specific field to search on (i.e. {'text': <query_text>, 'fields': [<field_to_search_on>]})"""

    underscore_name = "query"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: dict, query_type: QueryType, **options) -> ES_Q:
        nested_path = options.get("nested_path", "")
        query_text = filter_values["text"]
        query_fields = [f"{nested_path}{'.' if nested_path else ''}{field}" for field in filter_values["fields"]]
        return ES_Q("multi_match", query=query_text, type="phrase_prefix", fields=query_fields)


class _NonzeroFields(_Filter):
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


class _AwardUniqueId(_Filter):
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


class QueryWithFilters:

    @property
    def filter_lookup(self) -> dict[str, _Filter]:
        result = {
            _Keywords.underscore_name: _SubawardsKeywords if self.query_type == QueryType.SUBAWARDS else _Keywords,
            _Description.underscore_name: _Description,
            _KeywordSearch.underscore_name: _KeywordSearch,
            _TransactionKeywordSearch.underscore_name: _TransactionKeywordSearch,
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
            _ProgramActivities.underscore_name: _ProgramActivities,
            _AwardUniqueId.underscore_name: _AwardUniqueId,
        }
        if self.query_type == QueryType.SUBAWARDS:
            result[_SubawardsPrimeSubAwardTypes.underscore_name] = _SubawardsPrimeSubAwardTypes
        return result

    nested_filter_lookup = {
        f"nested_{_DisasterEmergencyFundCodes.underscore_name}": _DisasterEmergencyFundCodes,
        f"nested_{_QueryText.underscore_name}": _QueryText,
        f"nested_{_NonzeroFields.underscore_name}": _NonzeroFields,
    }

    unsupported_filters = ["legal_entities"]

    def __init__(self, query_type: QueryType):
        self.query_type = query_type
        time_period_obj = None
        if self.query_type == QueryType.ACCOUNTS:
            self.default_options = {"nested_path": "financial_accounts_by_award"}
        elif self.query_type == QueryType.TRANSACTIONS:
            time_period_obj = TransactionSearchTimePeriod(
                default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_MIN_DATE
            )
        elif self.query_type == QueryType.AWARDS:
            time_period_obj = AwardSearchTimePeriod(
                default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_MIN_DATE
            )
        elif self.query_type == QueryType.SUBAWARDS:
            time_period_obj = SubawardSearchTimePeriod(
                default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_MIN_DATE
            )

            self.default_options = {"time_period_obj": time_period_obj}

        if time_period_obj is not None and (
            self.query_type == QueryType.AWARDS or self.query_type == QueryType.TRANSACTIONS
        ):
            new_awards_only_decorator = NewAwardsOnlyTimePeriod(
                time_period_obj=time_period_obj, query_type=self.query_type
            )
            self.default_options = {"time_period_obj": new_awards_only_decorator}

    def generate_elasticsearch_query(self, filters: dict, **options) -> ES_Q:
        options = {**self.default_options, **options}
        nested_path = options.pop("nested_path", "")
        must_queries = []
        nested_must_queries = []

        # Create a copy of the filters so that manipulating the filters for the purpose of building the ES query
        # does not affect the source dictionary
        filters_copy = copy.deepcopy(filters)

        # tas_codes are unique in that the same query is spread across two keys
        must_queries = self._handle_tas_query(must_queries, filters_copy)
        for filter_type, filter_values in filters_copy.items():
            # Validate the filters
            if filter_type in self.unsupported_filters:
                msg = "API request included '{}' key. No filtering will occur with provided value '{}'"
                logger.warning(msg.format(filter_type, filter_values))
                continue
            elif filter_type not in self.filter_lookup.keys() and filter_type not in self.nested_filter_lookup.keys():
                raise InvalidParameterException(f"Invalid filter: {filter_type} does not exist.")

            # Generate the query for a filter
            if "nested_" in filter_type:
                # Add the "nested_path" option back in if using a nested filter;
                # want to avoid having this option passed to all filters
                nested_options = {**options, "nested_path": nested_path}
                query = self.nested_filter_lookup[filter_type].generate_query(
                    filter_values, self.query_type, **nested_options
                )
                list_pointer = nested_must_queries
            else:
                query = self.filter_lookup[filter_type].generate_query(filter_values, self.query_type, **options)
                list_pointer = must_queries

            # Handle the possibility of multiple queries from one filter
            if isinstance(query, list):
                list_pointer.extend(query)
            else:
                list_pointer.append(query)
        nested_query = ES_Q("nested", path="financial_accounts_by_award", query=ES_Q("bool", must=nested_must_queries))
        if must_queries and nested_must_queries:
            must_queries.append(nested_query)
        elif nested_must_queries:
            must_queries = nested_query
        return ES_Q("bool", must=must_queries)

    def _handle_tas_query(self, must_queries: list, filters: dict) -> list:
        if filters.get(TreasuryAccounts.underscore_name) or filters.get(TasCodes.underscore_name):
            tas_queries = []
            if filters.get(TreasuryAccounts.underscore_name):
                tas_queries.append(
                    TreasuryAccounts.generate_elasticsearch_query(
                        filters[TreasuryAccounts.underscore_name], self.query_type
                    )
                )
            if filters.get(TasCodes.underscore_name):
                tas_queries.append(
                    (TasCodes.generate_elasticsearch_query(filters[TasCodes.underscore_name], self.query_type))
                )
            must_queries.append(ES_Q("bool", should=tas_queries, minimum_should_match=1))
            filters.pop(TreasuryAccounts.underscore_name, None)
            filters.pop(TasCodes.underscore_name, None)
        return must_queries
