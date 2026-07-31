import itertools
import logging
from typing import Any

from django.db.models import Exists, OuterRef, Q, QuerySet

from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.awards.models.financial_accounts_by_awards import FinancialAccountsByAwards
from usaspending_api.awards.v2.filters.filter_helpers import combine_date_range_queryset, total_obligation_queryset
from usaspending_api.awards.v2.filters.location_filter_geocode import ALL_FOREIGN_COUNTRIES, create_nested_object
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import PSC
from usaspending_api.search.filters.postgres.defc import DefCodes
from usaspending_api.search.filters.postgres.psc import PSCCodes
from usaspending_api.search.filters.postgres.tas import TasCodes, TreasuryAccounts
from usaspending_api.search.helpers.matview_filter_helpers import build_award_ids_filter
from usaspending_api.search.models import SubawardSearch
from usaspending_api.search.v2 import elasticsearch_helper
from usaspending_api.settings import API_MAX_DATE, API_MIN_DATE, API_SEARCH_MIN_DATE

logger = logging.getLogger(__name__)


def subaward_download(filters: dict[str, Any]) -> QuerySet:
    """Used by the Custom download"""
    return subaward_filter(filters, for_downloads=True)


def geocode_filter_subaward_locations(scope: str, values: list) -> Q:
    """
    Function filter querysets for location data in subawards
    scope- place of performance or recipient location mappings
    values- array of location requests
    returns queryset
    """
    location_mappings = _get_location_mappings(scope)
    nested_values = create_nested_object(values)

    or_queryset = Q()
    for country, state_zip in nested_values.items():
        country_qs = _build_country_query(scope, country, location_mappings)
        state_qs = _build_state_queries(scope, state_zip, location_mappings)

        or_queryset |= (country_qs & state_qs) if country_qs else state_qs

    return or_queryset


def _get_location_mappings(scope: str) -> dict[str, str]:
    """Extract location field mappings for the given scope"""
    all_mappings = {
        "country_code": {"sub_legal_entity": "country_code", "sub_place_of_perform": "country_co"},
        "zip5": {"sub_legal_entity": "zip5", "sub_place_of_perform": "zip5"},
        "city_name": {"sub_legal_entity": "city_name", "sub_place_of_perform": "city_name"},
        "state_code": {"sub_legal_entity": "state_code", "sub_place_of_perform": "state_code"},
        "county_code": {"sub_legal_entity": "county_code", "sub_place_of_perform": "county_code"},
        "congressional_code": {"sub_legal_entity": "congressional", "sub_place_of_perform": "congressio"},
        "current_congressional_code": {
            "sub_legal_entity": "sub_legal_entity_congressional_current",
            "sub_place_of_perform": "sub_place_of_performance_congressional_current",
        },
    }
    return {location_type: field_dict[scope] for location_type, field_dict in all_mappings.items()}


def _build_country_query(scope: str, country: str, location_mappings: dict[str, str]) -> Q | None:
    """Build country-level query filter"""
    if country == ALL_FOREIGN_COUNTRIES:
        return None
    return Q(**{f"{scope}_{location_mappings['country_code']}__exact": country})


def _build_state_queries(scope: str, state_zip: dict, location_mappings: dict[str, str]) -> Q:
    """Build state-level query filters"""
    state_qs = Q()

    for state_zip_key, location_values in state_zip.items():
        if state_zip_key == "city":
            state_inner_qs = Q(**{f"{scope}_{location_mappings['city_name']}__in": location_values})
        elif state_zip_key == "zip":
            state_inner_qs = Q(**{f"{scope}_{location_mappings['zip5']}__in": location_values})
        else:
            state_inner_qs = _build_state_location_query(scope, state_zip_key, location_values, location_mappings)

        state_qs |= state_inner_qs

    return state_qs


def _build_state_location_query(
        scope: str, state_code: str, location_values: dict, location_mappings: dict[str, str]
) -> Q:
    """Build query for state with nested county/district/city filters"""
    state_qs = Q(**{f"{scope}_{location_mappings['state_code']}__exact": state_code.upper()})

    # Build sub-filters
    sub_filters = Q()

    if location_values.get("county"):
        sub_filters |= Q(**{f"{scope}_{location_mappings['county_code']}__in": location_values["county"]})

    if location_values.get("district_current"):
        sub_filters |= Q(
            **{f"{location_mappings['current_congressional_code']}__in": location_values["district_current"]}
        )

    if location_values.get("district_original"):
        sub_filters |= Q(
            **{f"{scope}_{location_mappings['congressional_code']}__in": location_values["district_original"]}
        )

    if location_values.get("city"):
        sub_filters |= Q(**{f"{scope}_{location_mappings['city_name']}__in": location_values["city"]})

    return state_qs & sub_filters


# TODO: Performance when multiple false values are initially provided
def subaward_filter(filters: dict[str, Any], for_downloads: bool = False) -> QuerySet:
    queryset = SubawardSearch.objects.all()

    recipient_scope_q = Q(sub_legal_entity_country_code="USA") | Q(sub_legal_entity_country_name="UNITED STATES")
    pop_scope_q = Q(sub_place_of_perform_country_co="USA") | Q(sub_place_of_perform_country_name="UNITED STATES")

    # Define valid filter keys
    valid_keys = [
        "keywords", "description", "transaction_keyword_search", "time_period",
        "award_type_codes", "prime_and_sub_award_types", "agencies", "legal_entities",
        "recipient_search_text", "recipient_scope", "recipient_locations",
        "recipient_type_names", "place_of_performance_scope", "place_of_performance_locations",
        "award_amounts", "award_ids", "program_numbers", "naics_codes",
        PSCCodes.underscore_name, "contract_pricing_type_codes",
        "set_aside_type_codes", "extent_competed_type_codes",
        TasCodes.underscore_name, TreasuryAccounts.underscore_name,
        "def_codes", "program_activities",
    ]

    # Create filter handlers mapping
    filter_handlers = {
        "keywords": _handle_keywords_filter,
        "description": _handle_description_filter,
        "transaction_keyword_search": _handle_transaction_keyword_filter,
        "time_period": lambda qs, val: _handle_time_period_filter(qs, val, for_downloads),
        "award_type_codes": _handle_award_type_codes_filter,
        "prime_and_sub_award_types": _handle_prime_and_sub_award_types_filter,
        "agencies": _handle_agencies_filter,
        "legal_entities": _handle_legal_entities_filter,
        "recipient_search_text": _handle_recipient_search_text_filter,
        "recipient_scope": lambda qs, val: _handle_scope_filter(qs, val, recipient_scope_q, "recipient_scope"),
        "recipient_locations": lambda qs, val: _handle_locations_filter(qs, val, "sub_legal_entity"),
        "recipient_type_names": _handle_recipient_type_names_filter,
        "place_of_performance_scope": lambda qs, val: _handle_scope_filter(qs, val, pop_scope_q,
                                                                           "place_of_performance_scope"),
        "place_of_performance_locations": lambda qs, val: _handle_locations_filter(qs, val, "sub_place_of_perform"),
        "award_amounts": lambda qs, val: _handle_award_amounts_filter(qs, val, filters),
        "award_ids": _handle_award_ids_filter,
        PSCCodes.underscore_name: _handle_psc_codes_filter,
        "contract_pricing_type_codes": _handle_contract_pricing_filter,
        "program_numbers": _handle_program_numbers_filter,
        "set_aside_type_codes": lambda qs, val: _handle_set_aside_extent_filter(qs, val, "set_aside_type_codes",
                                                                                "type_set_aside"),
        "extent_competed_type_codes": lambda qs, val: _handle_set_aside_extent_filter(qs, val,
                                                                                      "extent_competed_type_codes",
                                                                                      "extent_competed"),
        TasCodes.underscore_name: lambda qs, val: _handle_tas_codes_filter(qs, val, filters),
        "def_codes": _handle_def_codes_filter,
        "program_activities": _handle_program_activities_filter,
    }

    for key, value in filters.items():
        if value is None:
            raise InvalidParameterException(f"Invalid filter: {key} has null as its value.")

        if key not in valid_keys:
            raise InvalidParameterException(f"Invalid filter: {key} does not exist.")

        # Handle TreasuryAccounts special case
        if key == TreasuryAccounts.underscore_name and TasCodes.underscore_name not in filters:
            queryset = queryset.filter(TreasuryAccounts.build_tas_codes_filter(queryset, value))
            continue

        # Skip TreasuryAccounts if TasCodes is present (handled in TasCodes handler)
        if key == TreasuryAccounts.underscore_name:
            continue

        # Apply filter handler
        handler = filter_handlers.get(key)
        if handler:
            queryset = handler(queryset, value)

    return queryset


# Filter handler functions
def _handle_keywords_filter(queryset: QuerySet, value: list[str]) -> QuerySet:
    def keyword_parse(keyword: str) -> Q:
        filter_obj = Q(keyword_ts_vector=keyword) | Q(award_ts_vector=keyword)
        if len(keyword) == 4 and PSC.objects.filter(code__iexact=keyword).exists():
            filter_obj |= Q(product_or_service_code__iexact=keyword)
        return filter_obj

    filter_obj = Q()
    for keyword in value:
        filter_obj |= keyword_parse(keyword)

    # Search for DUNS
    potential_duns = [x for x in value if len(x) == 9]
    if potential_duns:
        filter_obj |= Q(sub_awardee_or_recipient_uniqu__in=potential_duns) | Q(
            sub_ultimate_parent_unique_ide__in=potential_duns
        )

    # Search for UEI
    potential_ueis = [uei.upper() for uei in value if len(uei) == 12]
    if potential_ueis:
        filter_obj |= Q(sub_awardee_or_recipient_uei__in=potential_ueis) | Q(
            sub_ultimate_parent_uei__in=potential_ueis
        )

    return queryset.filter(filter_obj)


def _handle_description_filter(queryset: QuerySet, value: str) -> QuerySet:
    return queryset.filter(subaward_description__icontains=value)


def _handle_transaction_keyword_filter(queryset: QuerySet, value: str) -> QuerySet:
    transaction_ids = elasticsearch_helper.get_download_ids(keyword=value, field="transaction_id")
    transaction_ids = list(itertools.chain.from_iterable(transaction_ids))
    logger.info(f"Found {len(transaction_ids)} transactions based on keyword: {value}")
    transaction_ids = [str(tid) for tid in transaction_ids]

    queryset = queryset.filter(latest_transaction__isnull=False)
    sql_fragment = '"subaward_search"."latest_transaction_id" = ANY(\'{{{}}}\'::int[])'
    return queryset.extra(where=[sql_fragment.format(",".join(transaction_ids))])


def _handle_time_period_filter(queryset: QuerySet, value: list[dict[str, Any]], for_downloads: bool) -> QuerySet:
    min_date = API_MIN_DATE if for_downloads else API_SEARCH_MIN_DATE
    return queryset & combine_date_range_queryset(value, SubawardSearch, min_date, API_MAX_DATE, is_subaward=True)


def _handle_award_type_codes_filter(queryset: QuerySet, value: list[str]) -> QuerySet:
    return queryset.filter(prime_award_type__in=value)


def _handle_prime_and_sub_award_types_filter(queryset: QuerySet, value: dict[str, Any]) -> QuerySet:
    award_types = value.get("sub_awards")
    return queryset.filter(prime_award_group__in=award_types) if award_types else queryset


def _handle_agencies_filter(queryset: QuerySet, value: list[dict[str, str]]) -> QuerySet:
    funding_toptier = Q()
    funding_subtier = Q()
    awarding_toptier = Q()
    awarding_subtier = Q()

    for v in value:
        agency_type = v["type"]
        tier = v["tier"]
        name = v["name"]

        if agency_type == "funding":
            if tier == "toptier":
                funding_toptier |= Q(funding_toptier_agency_name=name)
            elif tier == "subtier":
                base_q = Q(funding_subtier_agency_name=name)
                funding_subtier |= base_q & Q(
                    funding_toptier_agency_name=v["toptier_name"]) if "toptier_name" in v else base_q
        elif agency_type == "awarding":
            if tier == "toptier":
                awarding_toptier |= Q(awarding_toptier_agency_name=name)
            elif tier == "subtier":
                base_q = Q(awarding_subtier_agency_name=name)
                awarding_subtier |= base_q & Q(
                    awarding_toptier_agency_name=v["toptier_name"]) if "toptier_name" in v else base_q

    funding_filter = funding_toptier | funding_subtier
    awarding_filter = awarding_toptier | awarding_subtier

    return queryset.filter(funding_filter & awarding_filter)


def _handle_legal_entities_filter(queryset: QuerySet, value: Any) -> QuerySet:
    logger.info(f'API request included "legal_entities" key. No filtering will occur with provided value "{value}"')
    return queryset


def _handle_recipient_search_text_filter(queryset: QuerySet, value: list[str]) -> QuerySet:
    def recip_string_parse(recipient_string: str) -> Q:
        upper_recipient_string = recipient_string.upper()
        filter_obj = Q(recipient_name_ts_vector=upper_recipient_string)

        if len(upper_recipient_string) == 9 and upper_recipient_string[:5].isnumeric():
            filter_obj |= Q(sub_awardee_or_recipient_uniqu=upper_recipient_string)
        elif len(upper_recipient_string) == 12:
            filter_obj |= Q(sub_awardee_or_recipient_uei=upper_recipient_string)

        return filter_obj

    filter_obj = Q()
    for recipient in value:
        filter_obj |= recip_string_parse(recipient)

    return queryset.filter(filter_obj)


def _handle_scope_filter(queryset: QuerySet, value: str, scope_q: Q, filter_name: str) -> QuerySet:
    if value == "domestic":
        return queryset.filter(scope_q)
    if value == "foreign":
        return queryset.exclude(scope_q)
    raise InvalidParameterException(f"Invalid filter: {filter_name} type is invalid.")


def _handle_locations_filter(queryset: QuerySet, value: list[dict[str, Any]], prefix: str) -> QuerySet:
    return queryset.filter(geocode_filter_subaward_locations(prefix, value))


def _handle_recipient_type_names_filter(queryset: QuerySet, value: list[str]) -> QuerySet:
    return queryset.filter(business_categories__overlap=value) if value else queryset


def _handle_award_amounts_filter(queryset: QuerySet, value: dict[str, Any], filters: dict[str, Any]) -> QuerySet:
    return queryset & total_obligation_queryset(value, SubawardSearch, filters, is_subaward=True)


def _handle_award_ids_filter(queryset: QuerySet, value: list[str]) -> QuerySet:
    return build_award_ids_filter(queryset, value, ("piid", "fain"))


def _handle_psc_codes_filter(queryset: QuerySet, value: list[str]) -> QuerySet:
    q = PSCCodes.build_tas_codes_filter(value)
    return queryset.filter(q) if q else queryset


def _handle_contract_pricing_filter(queryset: QuerySet, value: list[str]) -> QuerySet:
    return queryset & SubawardSearch.objects.filter(type_of_contract_pricing__in=value) if value else queryset


def _handle_program_numbers_filter(queryset: QuerySet, value: list[str]) -> QuerySet:
    if not value:
        return queryset

    return queryset.filter(
        Exists(
            TransactionNormalized.objects.filter(
                award_id=OuterRef("award_id"),
                assistance_data__cfda_number__in=value,
            )
        )
    )


def _handle_set_aside_extent_filter(queryset: QuerySet, value: list[str], key: str, column: str) -> QuerySet:
    or_queryset = Q()
    for item in value:
        or_queryset |= Q(**{f"{column}__exact": item})
    return queryset.filter(or_queryset)


def _handle_tas_codes_filter(queryset: QuerySet, value: list[dict[str, Any]], filters: dict[str, Any]) -> QuerySet:
    q = TasCodes.build_tas_codes_filter(queryset, value)
    if TreasuryAccounts.underscore_name in filters:
        q |= TreasuryAccounts.build_tas_codes_filter(queryset, filters[TreasuryAccounts.underscore_name])
    return queryset.filter(q)


def _handle_def_codes_filter(queryset: QuerySet, value: list[str]) -> QuerySet:
    return queryset.filter(DefCodes.build_def_codes_filter(value))


def _handle_program_activities_filter(queryset: QuerySet, value: list[dict[str, str]]) -> QuerySet:
    query_filter_predicates = [Q(program_activity_id__isnull=False)]
    award_ids_filtered = []

    for program_activity in value:
        if "name" in program_activity:
            query_filter_predicates.append(
                Q(program_activity__program_activity_name=program_activity["name"].upper())
            )
        if "code" in program_activity:
            query_filter_predicates.append(
                Q(program_activity__program_activity_code=program_activity["code"])
            )

        filter_ = FinancialAccountsByAwards.objects.filter(*query_filter_predicates)
        award_ids_filtered.extend(list(filter_.values_list("award_id", flat=True)))

    return queryset & SubawardSearch.objects.filter(award_id__in=award_ids_filtered)
