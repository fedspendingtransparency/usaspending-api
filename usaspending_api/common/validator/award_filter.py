import copy

from django.conf import settings
from sys import maxsize
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.common.validator.helpers import TINY_SHIELD_SEPARATOR
from usaspending_api.search.filters.elasticsearch.psc import PSCCodes
from usaspending_api.search.filters.elasticsearch.tas import TasCodes, TreasuryAccounts


TIME_PERIOD_MIN_MESSAGE = (
    "%s falls before the earliest available search date of {min}.  For data going back to %s, use either the "
    "Custom Award Download feature on the website or one of our download or bulk_download API endpoints "
    "listed on https://api.usaspending.gov/docs/endpoints."
)

TAS_COMPONENTS_FILTER = {
    "ata": {"type": "text", "text_type": "search", "optional": True, "allow_nulls": True},
    "aid": {"type": "text", "text_type": "search", "optional": True, "allow_nulls": False},
    "bpoa": {"type": "text", "text_type": "search", "optional": True, "allow_nulls": True},
    "epoa": {"type": "text", "text_type": "search", "optional": True, "allow_nulls": True},
    "a": {"type": "text", "text_type": "search", "optional": True, "allow_nulls": True},
    "main": {"type": "text", "text_type": "search", "optional": True, "allow_nulls": False},
    "sub": {"type": "text", "text_type": "search", "optional": True, "allow_nulls": True},
}

STANDARD_FILTER_TREE_MODEL = {
    "type": "object",
    "min": 0,
    "object_keys": {
        "require": {
            "type": "array",
            "array_type": "any",
            "models": [{"type": "array", "array_type": "text", "text_type": "search"}],
            "min": 0,
        },
        "exclude": {
            "type": "array",
            "array_type": "any",
            "models": [{"type": "array", "array_type": "text", "text_type": "search"}],
            "min": 0,
        },
    },
}

AWARD_FILTER = [
    {"name": "award_ids", "type": "array", "array_type": "text", "text_type": "search"},
    {
        "name": "award_type_codes",
        "type": "array",
        "array_type": "enum",
        "enum_values": list(award_type_mapping.keys()) + ["no intersection"],
    },
    {"name": "contract_pricing_type_codes", "type": "array", "array_type": "text", "text_type": "search"},
    {"name": "extent_competed_type_codes", "type": "array", "array_type": "text", "text_type": "search"},
    {"name": "keywords", "type": "array", "array_type": "text", "text_type": "search", "text_min": 3},
    {"name": "legal_entities", "type": "array", "array_type": "integer", "array_max": maxsize},
    {
        "name": "naics_codes",
        "type": "any",
        "models": [
            {
                "name": "naics_codes",
                "type": "object",
                "min": 0,
                "object_keys": {
                    "require": {"type": "array", "array_type": "integer", "text_type": "search", "min": 0},
                    "exclude": {"type": "array", "array_type": "integer", "text_type": "search", "min": 0},
                },
            },
            {"type": "array", "array_type": "integer", "text_type": "search"},
        ],
    },
    {"name": "place_of_performance_scope", "type": "enum", "enum_values": ["domestic", "foreign"]},
    {"name": "program_numbers", "type": "array", "array_type": "text", "text_type": "search"},
    {
        "name": PSCCodes.underscore_name,
        "type": "any",
        "models": [
            {"type": "array", "array_type": "text", "text_type": "search", "min": 0},
            STANDARD_FILTER_TREE_MODEL,
        ],
    },
    {"name": "recipient_id", "type": "text", "text_type": "search"},
    {"name": "recipient_scope", "type": "enum", "enum_values": ("domestic", "foreign")},
    {"name": "recipient_search_text", "type": "array", "array_type": "text", "text_type": "search"},
    {"name": "recipient_type_names", "type": "array", "array_type": "text", "text_type": "search"},
    {"name": "set_aside_type_codes", "type": "array", "array_type": "text", "text_type": "search"},
    {
        "name": "time_period",
        "type": "array",
        "array_type": "object",
        "object_keys": {
            "start_date": {
                "type": "date",
                "min": settings.API_SEARCH_MIN_DATE,
                "min_exception": TIME_PERIOD_MIN_MESSAGE % ("start_date", settings.API_MIN_DATE),
                "max": settings.API_MAX_DATE,
            },
            "end_date": {
                "type": "date",
                "min": settings.API_SEARCH_MIN_DATE,
                "min_exception": TIME_PERIOD_MIN_MESSAGE % ("end_date", settings.API_MIN_DATE),
                "max": settings.API_MAX_DATE,
            },
            "date_type": {
                "type": "enum",
                "enum_values": ["action_date", "last_modified_date", "date_signed", "new_awards_only"],
                "optional": True,
            },
        },
    },
    {
        "name": "award_amounts",
        "type": "array",
        "array_type": "object",
        "object_keys": {
            "lower_bound": {"type": "float", "optional": True},
            "upper_bound": {"type": "float", "optional": True},
        },
    },
    {
        "name": "agencies",
        "type": "array",
        "array_type": "object",
        "object_keys": {
            "type": {"type": "enum", "enum_values": ["funding", "awarding"], "optional": False},
            "tier": {"type": "enum", "enum_values": ["toptier", "subtier"], "optional": False},
            "toptier_name": {"type": "text", "text_type": "search", "optional": True},
            "name": {"type": "text", "text_type": "search", "optional": False},
        },
    },
    {
        "name": "recipient_locations",
        "type": "array",
        "array_type": "object",
        "object_keys": {
            "country": {"type": "text", "text_type": "search", "optional": False},
            "state": {"type": "text", "text_type": "search", "optional": True},
            "zip": {"type": "text", "text_type": "search", "optional": True},
            "district_original": {
                "type": "text",
                "text_type": "search",
                "optional": True,
                "text_min": 2,
                "text_max": 2,
            },
            "district_current": {
                "type": "text",
                "text_type": "search",
                "optional": True,
                "text_min": 2,
                "text_max": 2,
            },
            "county": {"type": "text", "text_type": "search", "optional": True},
            "city": {"type": "text", "text_type": "search", "optional": True},
        },
    },
    {
        "name": "place_of_performance_locations",
        "type": "array",
        "array_type": "object",
        "object_keys": {
            "country": {"type": "text", "text_type": "search", "optional": False},
            "state": {"type": "text", "text_type": "search", "optional": True},
            "zip": {"type": "text", "text_type": "search", "optional": True},
            "district_original": {
                "type": "text",
                "text_type": "search",
                "optional": True,
                "text_min": 2,
                "text_max": 2,
            },
            "district_current": {
                "type": "text",
                "text_type": "search",
                "optional": True,
                "text_min": 2,
                "text_max": 2,
            },
            "county": {"type": "text", "text_type": "search", "optional": True},
            "city": {"type": "text", "text_type": "search", "optional": True},
        },
    },
    {
        "name": TreasuryAccounts.underscore_name,
        "type": "array",
        "array_type": "object",
        "object_keys": TAS_COMPONENTS_FILTER,
    },
    {
        "name": TasCodes.underscore_name,
        "type": "any",
        "models": [
            {"type": "array", "array_type": "object", "object_keys": TAS_COMPONENTS_FILTER},
            STANDARD_FILTER_TREE_MODEL,
        ],
    },
    {"name": "def_codes", "type": "array", "array_type": "text", "text_type": "search"},
    {"name": "description", "type": "text", "text_type": "search"},
    {"name": "award_unique_id", "type": "text", "text_type": "search"},
]

for a in AWARD_FILTER:
    a["optional"] = a.get("optional", True)  # future TODO: want to make time_period required
    a["key"] = "filters{sep}{name}".format(sep=TINY_SHIELD_SEPARATOR, name=a["name"])

AWARD_FILTER_NO_RECIPIENT_ID = [elem for elem in copy.deepcopy(AWARD_FILTER) if elem["name"] != "recipient_id"]
AWARD_FILTER_W_FILTERS = [elem for elem in copy.deepcopy(AWARD_FILTER)]
