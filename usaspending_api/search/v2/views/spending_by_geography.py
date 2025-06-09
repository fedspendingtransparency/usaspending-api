import copy
import logging
from decimal import Decimal
from enum import Enum
from typing import Optional

from django.conf import settings
from django.db.models import F, TextField, Value
from django.db.models.functions import Concat
from elasticsearch_dsl import A
from elasticsearch_dsl import Q as ES_Q
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.api_versioning import API_TRANSFORM_FUNCTIONS, api_transformations
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch, SubawardSearch, TransactionSearch
from usaspending_api.common.helpers.generic_helper import (
    get_generic_filters_message,
)
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import AWARD_FILTER_W_FILTERS
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.references.abbreviations import code_to_state, fips_to_code
from usaspending_api.references.models import PopCongressionalDistrict, PopCounty, RefCountryCode
from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.search.filters.time_period.decorators import NewAwardsOnlyTimePeriod
from usaspending_api.search.filters.time_period.query_types import (
    AwardSearchTimePeriod,
    SubawardSearchTimePeriod,
    TransactionSearchTimePeriod,
)
from usaspending_api.search.v2.elasticsearch_helper import (
    get_number_of_unique_terms,
    get_scaled_sum_aggregations,
)
from usaspending_api.search.v2.views.enums import SpendingLevel

logger = logging.getLogger(__name__)
API_VERSION = settings.API_VERSION


class GeoLayer(Enum):
    COUNTY = "county"
    DISTRICT = "district"
    STATE = "state"
    COUNTRY = "country"


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByGeographyVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns spending by state code, county code, or congressional district code.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_geography.md"

    agg_key: Optional[str]
    filters: dict
    geo_layer: GeoLayer
    geo_layer_filters: Optional[list[str]]
    obligation_column: str
    search_type: AwardSearch | SubawardSearch | TransactionSearch
    spending_level: Optional[SpendingLevel]

    @cache_response()
    def post(self, request: Request) -> Response:
        # First determine if we are using Subaward or Prime Awards / Transactions as this
        # will impact some of the downstream filters in the JSON request
        spending_type_models = [
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False},
            {
                "name": "spending_level",
                "key": "spending_level",
                "type": "enum",
                "enum_values": [
                    SpendingLevel.AWARD.value,
                    SpendingLevel.SUBAWARD.value,
                    SpendingLevel.TRANSACTION.value,
                ],
                "optional": True,
                "default": "transactions",
            },
        ]
        spending_level_tiny_shield = TinyShield(spending_type_models)
        spending_level_data = spending_level_tiny_shield.block(request.data)
        self.spending_level = SpendingLevel(
            "subawards" if spending_level_data["subawards"] else spending_level_data["spending_level"]
        )

        # Now step through the rest of filters, taking into account the spending_level
        program_activities_rule = [
            {
                "name": "program_activities",
                "type": "array",
                "key": "filters|program_activities",
                "object_keys_min": 1,
                "array_type": "object",
                "object_keys": {
                    "name": {"type": "text", "text_type": "search"},
                    "code": {"type": "text", "text_type": "search"},
                },
            }
        ]
        models = [
            {
                "name": "scope",
                "key": "scope",
                "type": "enum",
                "optional": False,
                "enum_values": ["place_of_performance", "recipient_location"],
            },
            {
                "name": "geo_layer",
                "key": "geo_layer",
                "type": "enum",
                "optional": False,
                "enum_values": [layer.value for layer in GeoLayer],
            },
            {
                "name": "geo_layer_filters",
                "key": "geo_layer_filters",
                "type": "array",
                "array_type": "text",
                "text_type": "search",
            },
        ]

        request_filter_rules = copy.deepcopy(AWARD_FILTER_W_FILTERS)
        if self.spending_level == SpendingLevel.SUBAWARD:
            for filter_rule in request_filter_rules:
                if filter_rule["name"] == "time_period":
                    filter_rule["object_keys"]["date_type"]["enum_values"] = [
                        "action_date",
                        "last_modified_date",
                        "date_signed",
                        "sub_action_date",
                    ]

        models.extend([*request_filter_rules, *program_activities_rule])
        original_filters = request.data.get("filters")
        tiny_shield = TinyShield(models)
        json_request = tiny_shield.block(request.data)
        if "filters" in json_request and "program_activities" in json_request["filters"]:
            tiny_shield.enforce_object_keys_min(json_request, program_activities_rule[0])

        model_dict = {
            "place_of_performance": {
                SpendingLevel.AWARD: "pop",
                SpendingLevel.SUBAWARD: "sub_pop",
                SpendingLevel.TRANSACTION: "pop",
            },
            "recipient_location": {
                SpendingLevel.AWARD: "recipient_location",
                SpendingLevel.SUBAWARD: "sub_recipient_location",
                SpendingLevel.TRANSACTION: "recipient_location",
            },
        }

        agg_key_dict = {
            "county": "county_agg_key",
            "district": "congressional_cur_agg_key",
        }
        if self.spending_level == SpendingLevel.SUBAWARD:
            agg_key_dict.update(
                {
                    "state": "state_code",
                    "country": "country_code",
                }
            )
        else:
            agg_key_dict.update(
                {
                    "state": "state_agg_key",
                    "country": "country_agg_key",
                }
            )

        scope = json_request["scope"]
        scope_field_name = model_dict[scope][self.spending_level]
        self.agg_key = f"{scope_field_name}_{agg_key_dict[json_request['geo_layer']]}"
        self.filters = json_request.get("filters")
        self.geo_layer = GeoLayer(json_request["geo_layer"])
        self.geo_layer_filters = json_request.get("geo_layer_filters")

        if scope_field_name == "pop":
            scope_filter_name = "place_of_performance_scope"
        else:
            scope_filter_name = "recipient_scope"

        # If searching for COUNTY, DISTRICT, or STATE then only search for values within
        #   USA, but don't overwrite a user's search.
        # DO add `recipient_scope` or `place_of_performance_scope` if it wasn't already included.
        #
        # If searching for COUNTRY and no scope was provided, then return results for all
        #   countries provided in the `geo_layer_filters` list.
        # DO NOT add `recipient_scope` or `place_of_performance_scope` to the filters, if it
        #   wasn't already included.
        if scope_filter_name not in self.filters and self.geo_layer != GeoLayer.COUNTRY:
            self.filters[scope_filter_name] = "domestic"

        if self.spending_level == SpendingLevel.SUBAWARD:
            self.search_type = SubawardSearch
            self.obligation_column = "subaward_amount"
            time_period_obj = SubawardSearchTimePeriod(
                default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_SEARCH_MIN_DATE
            )
            query_type = QueryType.SUBAWARDS
        else:
            self.obligation_column = "generated_pragmatic_obligation"
            if self.spending_level == SpendingLevel.AWARD:
                self.search_type = AwardSearch
                time_period_type = AwardSearchTimePeriod
                query_type = QueryType.AWARDS
            else:
                self.search_type = TransactionSearch
                time_period_type = TransactionSearchTimePeriod
                query_type = QueryType.TRANSACTIONS
            base_time_period_obj = time_period_type(
                default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_SEARCH_MIN_DATE
            )
            time_period_obj = NewAwardsOnlyTimePeriod(time_period_obj=base_time_period_obj, query_type=query_type)

        filter_options = {}
        filter_options["time_period_obj"] = time_period_obj

        query_with_filters = QueryWithFilters(query_type)
        filter_query = query_with_filters.generate_elasticsearch_query(self.filters, **filter_options)
        result = self.query_elasticsearch(filter_query)

        raw_response = {
            "scope": json_request["scope"],
            "geo_layer": self.geo_layer.value,
            "spending_level": self.spending_level.value,
            "results": result,
            "messages": [
                *get_generic_filters_message(original_filters.keys(), [elem["name"] for elem in models]),
                (
                    "The 'subawards' field will be deprecated in the future. "
                    "Set 'spending_level' to 'subawards' instead. See documentation for more information."
                ),
            ],
        }

        return Response(raw_response)

    def build_elasticsearch_search_with_aggregation(
        self, filter_query: ES_Q
    ) -> Optional[AwardSearch | SubawardSearch | TransactionSearch]:
        # Check number of unique terms (buckets) for performance and restrictions on maximum buckets allowed
        bucket_count = get_number_of_unique_terms(self.search_type, filter_query, f"{self.agg_key}.hash")
        if bucket_count == 0:
            return None

        # Define the aggregation
        # Add 100 to make sure that we consider enough records in each shard for accurate results
        group_by_agg_key = A("terms", field=self.agg_key, size=bucket_count, shard_size=bucket_count + 100)

        # Create the initial search using filters
        search = self.search_type().filter(filter_query)
        if self.spending_level == SpendingLevel.AWARD:
            # Add total outlays to aggregation
            total_outlays_sum_aggregations = get_scaled_sum_aggregations("total_outlays")
            total_outlays_sum_field = total_outlays_sum_aggregations["sum_field"]
            search.aggs.bucket("group_by_agg_key", group_by_agg_key).metric(
                "total_outlays_sum_field", total_outlays_sum_field
            )

        # Add obligation column to aggregation
        sum_aggregations = get_scaled_sum_aggregations(self.obligation_column)
        sum_field = sum_aggregations["sum_field"]
        search.aggs.bucket("group_by_agg_key", group_by_agg_key).metric("sum_field", sum_field)

        # Set size to 0 since we don't care about documents returned
        search.update_from_dict({"size": 0})

        return search

    def build_elasticsearch_result(self, response: dict) -> dict[str, dict]:
        def _key_to_geo_code(key):
            return f"{code_to_state[key[:2]]['fips']}{key[2:]}" if (key and key[:2] in code_to_state) else None

        # Get the codes
        geo_info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])
        if self.geo_layer == GeoLayer.COUNTRY:
            geo_codes = [bucket.get("key") for bucket in geo_info_buckets if bucket.get("key")]
        else:
            # Lookup the state FIPS codes
            geo_codes = [_key_to_geo_code(bucket["key"]) for bucket in geo_info_buckets if bucket.get("key")]

        # Get the current geo info
        current_geo_info = {}
        if self.geo_layer == GeoLayer.STATE:
            geo_info_query = (
                PopCounty.objects.filter(state_code__in=geo_codes, county_number="000")
                .annotate(
                    geo_code=F("state_code"),
                    display_name=F("state_name"),
                    population=F("latest_population"),
                    shape_code=F("state_code"),
                )
                .values("geo_code", "display_name", "population", "shape_code")
            )
        elif self.geo_layer == GeoLayer.COUNTY:
            geo_info_query = (
                PopCounty.objects.annotate(shape_code=Concat("state_code", "county_number", output_field=TextField()))
                .filter(shape_code__in=geo_codes)
                .annotate(
                    geo_code=F("county_number"),
                    display_name=F("county_name"),
                    population=F("latest_population"),
                )
                .values("geo_code", "display_name", "shape_code", "population")
            )
        elif self.geo_layer == GeoLayer.COUNTRY:
            geo_info_query = (
                RefCountryCode.objects.filter(country_code__in=geo_codes)
                .annotate(
                    shape_code=F("country_code"),
                    display_name=F("country_name"),
                    geo_code=F("country_code"),
                    population=F("latest_population"),
                )
                .values("geo_code", "display_name", "shape_code", "population")
            )
        else:
            geo_info_query = (
                PopCongressionalDistrict.objects.annotate(
                    shape_code=Concat("state_code", "congressional_district", output_field=TextField())
                )
                .filter(shape_code__in=geo_codes)
                .annotate(
                    geo_code=F("congressional_district"),
                    display_name=Concat(
                        "state_abbreviation", Value("-"), "congressional_district", output_field=TextField()
                    ),
                    population=F("latest_population"),
                )
                .values("geo_code", "display_name", "shape_code", "population")
            )
        for geo_info in geo_info_query.all():
            current_geo_info[geo_info["shape_code"]] = geo_info

        # Build out the results
        results = {}
        for bucket in geo_info_buckets:
            bucket_shape_code = (
                bucket.get("key") if self.geo_layer == GeoLayer.COUNTRY else _key_to_geo_code(bucket.get("key"))
            )
            geo_info = current_geo_info.get(bucket_shape_code) or {"shape_code": ""}

            if geo_info["shape_code"]:
                if self.geo_layer == GeoLayer.STATE:
                    geo_info["display_name"] = geo_info["display_name"].title()
                    geo_info["shape_code"] = fips_to_code[geo_info["shape_code"]].upper()
                elif self.geo_layer == GeoLayer.COUNTY or self.geo_layer == GeoLayer.COUNTRY:
                    geo_info["display_name"] = geo_info["display_name"].title()
                else:
                    geo_info["display_name"] = geo_info["display_name"].upper()

            per_capita = None
            aggregated_amount = int(bucket.get("sum_field", {"value": 0})["value"]) / Decimal("100")
            population = geo_info.get("population")
            if population:
                per_capita = (Decimal(aggregated_amount) / Decimal(population)).quantize(Decimal(".01"))

            results[geo_info["shape_code"]] = {
                "shape_code": geo_info["shape_code"],
                "display_name": geo_info.get("display_name"),
                "aggregated_amount": aggregated_amount,
                "population": population,
                "per_capita": per_capita,
            }
            if bucket.get("total_outlays_sum_field"):
                total_outlays = int(bucket.get("total_outlays_sum_field", {"value": 0})["value"]) / Decimal("100")
                results[geo_info["shape_code"]]["total_outlays"] = total_outlays
        return results

    def query_elasticsearch(self, filter_query: ES_Q) -> list:
        logger.info(filter_query.to_dict())
        search = self.build_elasticsearch_search_with_aggregation(filter_query)
        if search is None:
            return []
        response = search.handle_execute()
        results_dict = self.build_elasticsearch_result(response.aggs.to_dict())
        if self.geo_layer_filters:
            filtered_shape_codes = set(self.geo_layer_filters) & set(results_dict.keys())
            results = [results_dict[shape_code] for shape_code in filtered_shape_codes]
        else:
            results = results_dict.values()
        return results
